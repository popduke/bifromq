/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bifromq.dist.worker.cache;

import static java.util.Collections.singleton;
import static org.apache.bifromq.plugin.settingprovider.Setting.MaxGroupFanout;
import static org.apache.bifromq.plugin.settingprovider.Setting.MaxPersistentFanout;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import com.github.benmanes.caffeine.cache.Weigher;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.dist.worker.TopicIndex;
import org.apache.bifromq.dist.worker.cache.task.AddRoutesTask;
import org.apache.bifromq.dist.worker.cache.task.LoadEntryTask;
import org.apache.bifromq.dist.worker.cache.task.PatchLoadTask;
import org.apache.bifromq.dist.worker.cache.task.RefreshEntriesTask;
import org.apache.bifromq.dist.worker.cache.task.ReloadEntryTask;
import org.apache.bifromq.dist.worker.cache.task.RemoveRoutesTask;
import org.apache.bifromq.dist.worker.cache.task.TenantRouteCacheTask;
import org.apache.bifromq.dist.worker.schema.cache.GroupMatching;
import org.apache.bifromq.dist.worker.schema.cache.Matching;
import org.apache.bifromq.dist.worker.schema.cache.NormalMatching;
import org.apache.bifromq.metrics.ITenantMeter;
import org.apache.bifromq.metrics.TenantMetric;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.sysprops.props.DistMaxCachedRoutesPerTenant;
import org.apache.bifromq.type.RouteMatcher;
import org.apache.bifromq.util.TopicMatcher;
import org.checkerframework.checker.index.qual.NonNegative;
import org.jspecify.annotations.NonNull;

@Slf4j
class TenantRouteCache implements ITenantRouteCache {
    private final String tenantId;
    private final ISettingProvider settingProvider;
    private final ITenantRouteMatcher matcher;
    private final AsyncLoadingCache<RouteCacheKey, IMatchedRoutes> routesCache;
    private final TopicIndex<RouteCacheKey> index;
    private final Executor matchExecutor;
    private final ConcurrentLinkedDeque<TenantRouteCacheTask> patchTasks = new ConcurrentLinkedDeque<>();
    private final AtomicBoolean patchRunning = new AtomicBoolean(false);
    private final AtomicLong mutationSeq = new AtomicLong();
    private final AtomicLong lastAppliedPatch = new AtomicLong();
    private final AtomicInteger inflightCount = new AtomicInteger();
    private final AtomicInteger pendingLoads = new AtomicInteger();
    private final ConcurrentSkipListMap<Long, AtomicInteger> inflightLoadStartSeqs = new ConcurrentSkipListMap<>();
    private final PatchLog patchLog = new PatchLog(); // keep track of patch mutations during loads
    private final String[] tags;
    private final Timer patchLatencyTimer;
    private final Timer loadLatencyTimer;

    TenantRouteCache(KVRangeId rangeId,
                     String tenantId,
                     ITenantRouteMatcher matcher,
                     ISettingProvider settingProvider,
                     Duration expiryAfterAccess,
                     Duration fanoutCheckInterval,
                     Executor matchExecutor) {
        this(rangeId, tenantId, matcher, settingProvider, expiryAfterAccess, fanoutCheckInterval, Ticker.systemTicker(),
            matchExecutor);
    }

    TenantRouteCache(KVRangeId rangeId,
                     String tenantId,
                     ITenantRouteMatcher matcher,
                     ISettingProvider settingProvider,
                     Duration expiryAfterAccess,
                     Duration fanoutCheckInterval,
                     Ticker ticker,
                     Executor matchExecutor) {
        this.tenantId = tenantId;
        this.matcher = matcher;
        this.matchExecutor = matchExecutor;
        this.settingProvider = settingProvider;
        index = new TopicIndex<>();
        routesCache = Caffeine.newBuilder()
            .scheduler(Scheduler.systemScheduler())
            .ticker(ticker)
            .executor(matchExecutor)
            .maximumWeight(DistMaxCachedRoutesPerTenant.INSTANCE.get())
            .weigher(new Weigher<RouteCacheKey, IMatchedRoutes>() {
                @Override
                public @NonNegative int weigh(RouteCacheKey key, IMatchedRoutes value) {
                    return Math.max(1, value.routes().size());
                }
            })
            .expireAfterAccess(expiryAfterAccess)
            .refreshAfterWrite(fanoutCheckInterval)
            .removalListener(
                (RouteCacheKey key, IMatchedRoutes value, RemovalCause cause) -> index.remove(key.topic, key))
            .recordStats()
            .buildAsync(new AsyncCacheLoader<>() {
                @Override
                public CompletableFuture<IMatchedRoutes> asyncLoad(RouteCacheKey key, Executor executor) {
                    pendingLoads.incrementAndGet();
                    long startSeq = lastAppliedPatch.get();
                    recordInflightLoad(startSeq);
                    inflightCount.incrementAndGet();
                    pendingLoads.decrementAndGet();
                    LoadEntryTask task = LoadEntryTask.of(key.topic, key, startSeq);
                    matchExecutor.execute(() -> runLoadTask(task));
                    return task.future;
                }

                @Override
                public CompletableFuture<IMatchedRoutes> asyncReload(RouteCacheKey key,
                                                                     @NonNull IMatchedRoutes oldValue,
                                                                     Executor executor) {
                    int maxPersistentFanouts = settingProvider.provide(MaxPersistentFanout, tenantId);
                    int maxGroupFanouts = settingProvider.provide(MaxGroupFanout, tenantId);
                    if (oldValue.adjust(maxPersistentFanouts, maxGroupFanouts)
                        == IMatchedRoutes.AdjustResult.ReloadNeeded) {
                        ReloadEntryTask task = ReloadEntryTask.of(key.topic, key, maxPersistentFanouts,
                            maxGroupFanouts);
                        submitPatchTask(task);
                        return task.future;
                    }
                    return CompletableFuture.completedFuture(oldValue);
                }
            });
        tags = new String[] {"id", KVRangeIdUtil.toString(rangeId)};
        Tags meterTags = Tags.of(tags).and(ITenantMeter.TAG_TENANT_ID, tenantId);
        patchLatencyTimer = Timer.builder(TenantMetric.MqttRouteCachePatchLatency.metricName)
            .tags(meterTags)
            .register(Metrics.globalRegistry);
        loadLatencyTimer = Timer.builder(TenantMetric.MqttRouteCacheLoadLatency.metricName)
            .tags(meterTags)
            .register(Metrics.globalRegistry);
        ITenantMeter.counting(tenantId, TenantMetric.MqttRouteCacheHitCount, routesCache.synchronous(),
            cache -> cache.stats().hitCount(), tags);
        ITenantMeter.counting(tenantId, TenantMetric.MqttRouteCacheMissCount, routesCache.synchronous(),
            cache -> cache.stats().missCount(), tags);
        ITenantMeter.counting(tenantId, TenantMetric.MqttRouteCacheEvictCount, routesCache.synchronous(),
            cache -> cache.stats().evictionCount(), tags);
        ITenantMeter.gauging(tenantId, TenantMetric.MqttRouteCacheSize, routesCache.synchronous()::estimatedSize, tags);
    }

    @Override
    public boolean isCached(List<String> filterLevels) {
        return !index.match(filterLevels).isEmpty();
    }

    @Override
    public void refresh(RefreshEntriesTask task) {
        submitPatchTask(task);
    }

    private void submitPatchTask(TenantRouteCacheTask task) {
        patchTasks.add(task);
        trySchedulePatchLoop();
    }

    private void submitPatchTaskFirst(TenantRouteCacheTask task) {
        patchTasks.addFirst(task);
        trySchedulePatchLoop();
    }

    private void trySchedulePatchLoop() {
        if (patchRunning.compareAndSet(false, true)) {
            matchExecutor.execute(this::runPatchLoop);
        }
    }

    private void runLoadTask(LoadEntryTask loadEntryTask) {
        String topic = loadEntryTask.topic;
        RouteCacheKey cacheKey = loadEntryTask.cacheKey;
        int maxPersistentFanouts = settingProvider.provide(MaxPersistentFanout, tenantId);
        int maxGroupFanouts = settingProvider.provide(MaxGroupFanout, tenantId);
        Map<String, IMatchedRoutes> results =
            matcher.matchAll(singleton(topic), maxPersistentFanouts, maxGroupFanouts);
        IMatchedRoutes matchedRoutes = results.get(topic);
        cacheKey.cachedMatchedRoutes.set(matchedRoutes);
        loadEntryTask.future.complete(matchedRoutes);
        recordLoadLatency(loadEntryTask);
        submitPatchTaskFirst(PatchLoadTask.of(cacheKey, loadEntryTask.startSeq, loadEntryTask.future));
    }

    private void runPatchLoop() {
        TenantRouteCacheTask task;
        while ((task = patchTasks.poll()) != null) {
            switch (task.type()) {
                case AddRoutes -> handleAddRoutes((AddRoutesTask) task);
                case RemoveRoutes -> handleRemoveRoutes((RemoveRoutesTask) task);
                case Reload, PatchLoad -> {
                    List<TenantRouteCacheTask> batch = new ArrayList<>();
                    batch.add(task);
                    while (true) {
                        TenantRouteCacheTask next = patchTasks.peek();
                        if (next == null || !isParallelTask(next)) {
                            break;
                        }
                        batch.add(patchTasks.poll());
                    }
                    runParallelBatch(batch);
                    return;
                }
                default -> {
                    // do nothing
                }
            }
        }
        patchRunning.set(false);
        if (!patchTasks.isEmpty()) {
            trySchedulePatchLoop();
        }
    }

    private boolean isParallelTask(TenantRouteCacheTask task) {
        return switch (task.type()) {
            case Reload, PatchLoad -> true;
            default -> false;
        };
    }

    private void runParallelBatch(List<TenantRouteCacheTask> batch) {
        // same key serial, different key parallel
        Map<RouteCacheKey, CompletableFuture<Void>> serialTasks = new HashMap<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>(batch.size());
        for (TenantRouteCacheTask task : batch) {
            RouteCacheKey cacheKey = cacheKeyFor(task);
            CompletableFuture<Void> tail = serialTasks.get(cacheKey);
            CompletableFuture<Void> next = tail == null
                ? CompletableFuture.runAsync(() -> handleParallelTask(task), matchExecutor)
                : tail.thenRunAsync(() -> handleParallelTask(task), matchExecutor);
            serialTasks.put(cacheKey, next);
            futures.add(next);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .whenComplete((result, error) -> {
                cleanupPatchLog();
                patchRunning.set(false);
                if (!patchTasks.isEmpty()) {
                    trySchedulePatchLoop();
                }
            });
    }

    private RouteCacheKey cacheKeyFor(TenantRouteCacheTask task) {
        if (task instanceof ReloadEntryTask reloadEntryTask) {
            return reloadEntryTask.cacheKey;
        }
        if (task instanceof PatchLoadTask patchLoadTask) {
            return patchLoadTask.cacheKey;
        }
        throw new IllegalStateException("Unexpected task type: " + task.type());
    }

    private void handleParallelTask(TenantRouteCacheTask task) {
        try {
            switch (task.type()) {
                case Reload -> handleReload((ReloadEntryTask) task);
                case PatchLoad -> handlePatchLoad((PatchLoadTask) task);
                default -> {
                    // do nothing
                }
            }
        } finally {
            recordPatchLatency(task);
        }
    }

    private void handleAddRoutes(AddRoutesTask addTask) {
        applyRoutesToMatchedKeys(addTask.routes, true);
        long seq = mutationSeq.incrementAndGet();
        lastAppliedPatch.set(seq);
        if (inflightCount.get() > 0 || pendingLoads.get() > 0) {
            patchLog.append(seq, addTask.routes, true);

        }
        cleanupPatchLog();
        recordPatchLatency(addTask);
    }

    private void handleRemoveRoutes(RemoveRoutesTask removeTask) {
        applyRoutesToMatchedKeys(removeTask.routes, false);
        long seq = mutationSeq.incrementAndGet();
        lastAppliedPatch.set(seq);
        if (inflightCount.get() > 0 || pendingLoads.get() > 0) {
            patchLog.append(seq, removeTask.routes, false);
        }
        cleanupPatchLog();
        recordPatchLatency(removeTask);
    }

    private void handleReload(ReloadEntryTask reloadEntryTask) {
        String topic = reloadEntryTask.topic;
        RouteCacheKey cacheKey = reloadEntryTask.cacheKey;
        Map<String, IMatchedRoutes> results = matcher.matchAll(singleton(topic),
            reloadEntryTask.maxPersistentFanouts, reloadEntryTask.maxGroupFanouts);
        IMatchedRoutes matchedRoutes = results.get(topic);
        cacheKey.cachedMatchedRoutes.set(matchedRoutes);
        reloadEntryTask.future.complete(matchedRoutes);
    }

    private void handlePatchLoad(PatchLoadTask patchLoadTask) {
        RouteCacheKey cacheKey = patchLoadTask.cacheKey;
        CompletableFuture<IMatchedRoutes> cachedFuture = routesCache.asMap().get(cacheKey);
        // entry is still in cache
        if (cachedFuture == patchLoadTask.loadFuture) {
            long endSeq = lastAppliedPatch.get();
            replayPatchLog(patchLoadTask.startSeq, endSeq, cacheKey);
            index.add(cacheKey.topic, cacheKey);
            CompletableFuture<IMatchedRoutes> currentFuture = routesCache.asMap().get(cacheKey);
            if (currentFuture != patchLoadTask.loadFuture) {
                // if entry is expired after indexing, remove it anyway
                index.remove(cacheKey.topic, cacheKey);
            }
        }
        completeInflightLoad(patchLoadTask.startSeq);
        inflightCount.decrementAndGet();
    }

    private void recordInflightLoad(long startSeq) {
        inflightLoadStartSeqs.compute(startSeq, (seq, count) -> {
            if (count == null) {
                return new AtomicInteger(1);
            }
            count.incrementAndGet();
            return count;
        });
    }

    private void completeInflightLoad(long startSeq) {
        inflightLoadStartSeqs.computeIfPresent(startSeq, (seq, count) -> {
            if (count.decrementAndGet() <= 0) {
                return null;
            }
            return count;
        });
    }

    private void applyRoutesToMatchedKeys(Map<RouteMatcher, Set<Matching>> routes, boolean isAdd) {
        for (RouteMatcher topicFilter : routes.keySet()) {
            Set<Matching> matchings = routes.get(topicFilter);
            List<String> filterLevels = topicFilter.getFilterLevelList();
            Set<RouteCacheKey> keys = index.match(filterLevels);
            for (RouteCacheKey cacheKey : keys) {
                applyRoutesToCacheKey(cacheKey, topicFilter, matchings, isAdd);
            }
        }
    }

    private void applyRoutesToCacheKey(RouteCacheKey cacheKey,
                                       RouteMatcher topicFilter,
                                       Set<Matching> matchings,
                                       boolean isAdd) {
        if (isAdd) {
            addMatchings(cacheKey, topicFilter, matchings);
        } else {
            removeMatchings(cacheKey, topicFilter, matchings);
        }
    }

    private void addMatchings(RouteCacheKey cacheKey, RouteMatcher topicFilter, Set<Matching> matchings) {
        IMatchedRoutes matchedRoutes = cacheKey.cachedMatchedRoutes.get();
        switch (topicFilter.getType()) {
            case Normal -> {
                for (Matching matching : matchings) {
                    matchedRoutes.addNormalMatching((NormalMatching) matching);
                }
            }
            case OrderedShare, UnorderedShare -> {
                for (Matching matching : matchings) {
                    GroupMatching groupMatching = (GroupMatching) matching;
                    matchedRoutes.putGroupMatching(groupMatching);
                }
            }
            default -> {
                // do nothing
            }
        }

    }

    private void removeMatchings(RouteCacheKey cacheKey, RouteMatcher topicFilter, Set<Matching> matchings) {
        IMatchedRoutes matchedRoutes = cacheKey.cachedMatchedRoutes.get();
        switch (topicFilter.getType()) {
            case Normal -> {
                for (Matching matching : matchings) {
                    matchedRoutes.removeNormalMatching((NormalMatching) matching);
                }
            }
            case OrderedShare, UnorderedShare -> {
                for (Matching matching : matchings) {
                    GroupMatching groupMatching = (GroupMatching) matching;
                    if (groupMatching.receivers().isEmpty()) {
                        matchedRoutes.removeGroupMatching(groupMatching);
                    } else {
                        matchedRoutes.putGroupMatching(groupMatching);
                    }
                }
            }
            default -> {
                // do nothing
            }
        }

    }

    private void replayPatchLog(long startSeq, long endSeq, RouteCacheKey cacheKey) {
        if (patchLog.isEmpty() || startSeq >= endSeq) {
            return;
        }
        TopicMatcher matcher = new TopicMatcher(cacheKey.topic);
        patchLog.forEach(startSeq, endSeq, record -> {
            for (RouteMatcher topicFilter : record.routes.keySet()) {
                if (!matcher.match(topicFilter)) {
                    continue;
                }
                Set<Matching> matchings = record.routes.get(topicFilter);
                applyRoutesToCacheKey(cacheKey, topicFilter, matchings, record.isAdd);
            }
        });
    }

    private void cleanupPatchLog() {
        if (patchLog.isEmpty()) {
            return;
        }
        if (pendingLoads.get() > 0) {
            return;
        }
        if (inflightCount.get() == 0) {
            patchLog.clear();
            return;
        }
        Map.Entry<Long, AtomicInteger> entry = inflightLoadStartSeqs.firstEntry();
        if (entry == null) {
            return;
        }
        patchLog.trimBefore(entry.getKey() + 1);
    }

    private void recordPatchLatency(TenantRouteCacheTask task) {
        patchLatencyTimer.record(System.nanoTime() - task.startNanos, TimeUnit.NANOSECONDS);
    }

    private void recordLoadLatency(LoadEntryTask task) {
        loadLatencyTimer.record(System.nanoTime() - task.startNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public CompletableFuture<Set<Matching>> getMatch(String topic, Boundary currentTenantRange) {
        return routesCache.get(new RouteCacheKey(topic, currentTenantRange)).thenApply(IMatchedRoutes::routes);
    }

    @Override
    public void destroy() {
        routesCache.synchronous().asMap().keySet().forEach(key -> index.remove(key.topic, key));
        routesCache.synchronous().invalidateAll();
        ITenantMeter.stopCounting(tenantId, TenantMetric.MqttRouteCacheMissCount, tags);
        ITenantMeter.stopCounting(tenantId, TenantMetric.MqttRouteCacheHitCount, tags);
        ITenantMeter.stopCounting(tenantId, TenantMetric.MqttRouteCacheEvictCount, tags);
        ITenantMeter.stopGauging(tenantId, TenantMetric.MqttRouteCacheSize, tags);
        Metrics.globalRegistry.remove(patchLatencyTimer);
        Metrics.globalRegistry.remove(loadLatencyTimer);
    }
}
