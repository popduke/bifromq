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

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.apache.bifromq.dist.worker.schema.cache.Matchings.normalMatching;
import static org.apache.bifromq.dist.worker.schema.cache.Matchings.receiverUrl;
import static org.apache.bifromq.dist.worker.schema.cache.Matchings.unorderedGroupMatching;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.github.benmanes.caffeine.cache.Ticker;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.dist.worker.Comparators;
import org.apache.bifromq.dist.worker.MeterTest;
import org.apache.bifromq.dist.worker.cache.task.AddRoutesTask;
import org.apache.bifromq.dist.worker.cache.task.RemoveRoutesTask;
import org.apache.bifromq.dist.worker.schema.cache.GroupMatching;
import org.apache.bifromq.dist.worker.schema.cache.Matching;
import org.apache.bifromq.dist.worker.schema.cache.NormalMatching;
import org.apache.bifromq.metrics.TenantMetric;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.apache.bifromq.sysprops.props.DistMaxCachedRoutesPerTenant;
import org.apache.bifromq.type.RouteMatcher;
import org.apache.bifromq.util.TopicUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TenantRouteCacheTest extends MeterTest {
    private static final KVRangeId RANGE_ID = KVRangeIdUtil.generate();
    private static final String TENANT_ID = "tenantA";
    private static final String TOPIC = "sensor/temperature";
    private static final Duration EXPIRY = Duration.ofMinutes(1);
    private static final Duration FANOUT_CHECK = Duration.ofMillis(200);

    private final Set<TenantRouteCache> caches = ConcurrentHashMap.newKeySet();

    private ManualTicker ticker;
    private ITenantRouteMatcher matcher;
    private IEventCollector eventCollector;
    private ISettingProvider settingProvider;
    private ExecutorService executorToShutdown;

    @BeforeMethod
    public void setUp() {
        super.setup();
        ticker = new ManualTicker();
        matcher = mock(ITenantRouteMatcher.class);
        settingProvider = mock(ISettingProvider.class);
        eventCollector = mock(IEventCollector.class);
        executorToShutdown = null;
    }

    @AfterMethod
    public void tearDown() {
        caches.forEach(TenantRouteCache::destroy);
        caches.clear();
        if (executorToShutdown != null) {
            executorToShutdown.shutdownNow();
            executorToShutdown = null;
        }
        super.tearDown();
    }

    @Test
    public void shouldLoadAndIndexRoutesOnFirstAccess() {
        executorToShutdown = Executors.newSingleThreadExecutor();
        TenantRouteCache cache = newCache(executorToShutdown);
        NormalMatching existing = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        IMatchedRoutes matchedRoutes = mockRoutesWithBackingSet(10, 5, Set.of(existing));

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(10);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5);
        when(matcher.matchAll(eq(Set.of(TOPIC)), eq(10), eq(5))).thenReturn(Map.of(TOPIC, matchedRoutes));

        Set<Matching> result = cache.getMatch(TOPIC, FULL_BOUNDARY).join();

        assertEquals(result, Set.of(existing));
        await().atMost(Duration.ofSeconds(5))
            .until(() -> cache.isCached(TopicUtil.from(TOPIC).getFilterLevelList()));
        verify(matcher, times(1)).matchAll(eq(Set.of(TOPIC)), eq(10), eq(5));
    }

    @Test
    public void shouldReuseCachedValueWithoutReload() {
        TenantRouteCache cache = newCache(directExecutor());
        NormalMatching existing = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        IMatchedRoutes matchedRoutes = mockRoutesWithBackingSet(10, 5, Set.of(existing));

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(10);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5);
        when(matcher.matchAll(eq(Set.of(TOPIC)), eq(10), eq(5))).thenReturn(Map.of(TOPIC, matchedRoutes));

        Set<Matching> first = cache.getMatch(TOPIC, FULL_BOUNDARY).join();
        Set<Matching> second = cache.getMatch(TOPIC, FULL_BOUNDARY).join();

        assertEquals(first, Set.of(existing));
        assertEquals(second, Set.of(existing));
        verify(matcher, times(1)).matchAll(eq(Set.of(TOPIC)), eq(10), eq(5));
    }

    @Test
    public void shouldReloadWhensReloadReturned() {
        TenantRouteCache cache = newCache(directExecutor());
        NormalMatching existing = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        NormalMatching extra = normalMatching(TENANT_ID, TOPIC, 1, "receiverB", "delivererB", 2);
        IMatchedRoutes initial = mockRoutesWithBackingSet(1, 5, Set.of(existing));
        IMatchedRoutes reloaded = mockRoutesWithBackingSet(2, 5, Set.of(existing, extra));

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(1, 2);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5, 5);
        when(matcher.matchAll(eq(Set.of(TOPIC)), eq(1), eq(5))).thenReturn(Map.of(TOPIC, initial));
        when(matcher.matchAll(eq(Set.of(TOPIC)), eq(2), eq(5))).thenReturn(Map.of(TOPIC, reloaded));

        Set<Matching> first = cache.getMatch(TOPIC, FULL_BOUNDARY).join();
        assertEquals(first, Set.of(existing));

        ticker.advance(FANOUT_CHECK.plusMillis(1));
        await().atMost(Duration.ofSeconds(5)).until(() -> cache.getMatch(TOPIC, FULL_BOUNDARY).join().contains(extra));

        verify(matcher, times(2)).matchAll(eq(Set.of(TOPIC)), anyInt(), anyInt());
    }

    @Test
    public void shouldNotReloadWhenAdjustedReturned() {
        TenantRouteCache cache = newCache(directExecutor());
        NormalMatching existing = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        IMatchedRoutes matchedRoutes = mockRoutesWithBackingSet(10, 5, Set.of(existing));

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(10, 20);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5, 5);
        when(matcher.matchAll(eq(Set.of(TOPIC)), eq(10), eq(5))).thenReturn(Map.of(TOPIC, matchedRoutes));

        Set<Matching> first = cache.getMatch(TOPIC, FULL_BOUNDARY).join();
        assertEquals(first, Set.of(existing));

        ticker.advance(FANOUT_CHECK.plusMillis(1));
        Set<Matching> second = cache.getMatch(TOPIC, FULL_BOUNDARY).join();
        assertEquals(second, Set.of(existing));

        verify(matcher, times(1)).matchAll(eq(Set.of(TOPIC)), anyInt(), anyInt());
    }

    @Test
    public void shouldDeferAddRoutesUntilReloadBatchComplete() {
        ManualExecutor executor = new ManualExecutor();
        TenantRouteCache cache = newCache(executor);
        String topicA = "sensor/temp";
        String topicB = "sensor/humidity";

        NormalMatching existingA = normalMatching(TENANT_ID, topicA, 1, "receiverA", "delivererA", 1);
        NormalMatching existingB = normalMatching(TENANT_ID, topicB, 1, "receiverB", "delivererB", 1);
        IMatchedRoutes initialA = mockRoutesWithBackingSet(topicA, 1, 5, Set.of(existingA));
        IMatchedRoutes initialB = mockRoutesWithBackingSet(topicB, 1, 5, Set.of(existingB));
        IMatchedRoutes reloadedA = mockRoutesWithBackingSet(topicA, 2, 5, Set.of(existingA));
        IMatchedRoutes reloadedB = mockRoutesWithBackingSet(topicB, 2, 5, Set.of(existingB));

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID)))
            .thenReturn(1, 1, 2, 2);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5);

        Map<String, AtomicInteger> callsByTopic = new ConcurrentHashMap<>();
        when(matcher.matchAll(any(), anyInt(), anyInt())).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Set<String> topics = invocation.getArgument(0);
            String topic = topics.iterator().next();
            int call = callsByTopic.computeIfAbsent(topic, ignore -> new AtomicInteger()).incrementAndGet();
            if (topic.equals(topicA)) {
                return call == 1 ? Map.of(topicA, initialA) : Map.of(topicA, reloadedA);
            }
            return call == 1 ? Map.of(topicB, initialB) : Map.of(topicB, reloadedB);
        });

        cache.getMatch(topicA, FULL_BOUNDARY);
        cache.getMatch(topicB, FULL_BOUNDARY);
        executor.runAll();

        assertTrue(cache.isCached(TopicUtil.from(topicA).getFilterLevelList()));
        assertTrue(cache.isCached(TopicUtil.from(topicB).getFilterLevelList()));

        ticker.advance(FANOUT_CHECK.plusMillis(1));
        cache.getMatch(topicA, FULL_BOUNDARY);
        cache.getMatch(topicB, FULL_BOUNDARY);

        NormalMatching newRoute = normalMatching(TENANT_ID, topicA, 1, "receiverC", "delivererC", 2);
        NavigableMap<RouteMatcher, Set<Matching>> additions = new TreeMap<>(Comparators.RouteMatcherComparator);
        additions.put(newRoute.matcher, Set.of(newRoute));
        cache.refresh(AddRoutesTask.of(additions));

        int guard = 20;
        while (guard-- > 0) {
            int countA = callCount(callsByTopic, topicA);
            int countB = callCount(callsByTopic, topicB);
            if (countA >= 2 && countB >= 2) {
                break;
            }
            assertFalse(initialA.routes().contains(newRoute));
            assertFalse(reloadedA.routes().contains(newRoute));
            executor.runNext();
        }
        assertTrue(callCount(callsByTopic, topicA) >= 2);
        assertTrue(callCount(callsByTopic, topicB) >= 2);
        assertFalse(reloadedA.routes().contains(newRoute));

        executor.runAll();
        assertTrue(reloadedA.routes().contains(newRoute));
    }

    @Test
    public void shouldReloadInParallelAcrossKeys() {
        ManualExecutor executor = new ManualExecutor();
        TenantRouteCache cache = newCache(executor);
        String topicA = "sensor/temp";
        String topicB = "sensor/humidity";

        NormalMatching existingA = normalMatching(TENANT_ID, topicA, 1, "receiverA", "delivererA", 1);
        NormalMatching existingB = normalMatching(TENANT_ID, topicB, 1, "receiverB", "delivererB", 1);
        IMatchedRoutes initialA = mockRoutesWithBackingSet(topicA, 1, 5, Set.of(existingA));
        IMatchedRoutes initialB = mockRoutesWithBackingSet(topicB, 1, 5, Set.of(existingB));
        IMatchedRoutes reloadedA = mockRoutesWithBackingSet(topicA, 2, 5, Set.of(existingA));
        IMatchedRoutes reloadedB = mockRoutesWithBackingSet(topicB, 2, 5, Set.of(existingB));

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID)))
            .thenReturn(1, 1, 2, 2);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5);

        Map<String, AtomicInteger> callsByTopic = new ConcurrentHashMap<>();
        when(matcher.matchAll(any(), anyInt(), anyInt())).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Set<String> topics = invocation.getArgument(0);
            String topic = topics.iterator().next();
            int call = callsByTopic.computeIfAbsent(topic, ignore -> new AtomicInteger()).incrementAndGet();
            if (topic.equals(topicA)) {
                return call == 1 ? Map.of(topicA, initialA) : Map.of(topicA, reloadedA);
            }
            return call == 1 ? Map.of(topicB, initialB) : Map.of(topicB, reloadedB);
        });

        cache.getMatch(topicA, FULL_BOUNDARY);
        cache.getMatch(topicB, FULL_BOUNDARY);
        executor.runAll();

        assertTrue(cache.isCached(TopicUtil.from(topicA).getFilterLevelList()));
        assertTrue(cache.isCached(TopicUtil.from(topicB).getFilterLevelList()));

        ticker.advance(FANOUT_CHECK.plusMillis(1));
        cache.getMatch(topicA, FULL_BOUNDARY);
        cache.getMatch(topicB, FULL_BOUNDARY);

        executor.runNext();
        assertTrue(executor.pending() >= 2);
        executor.runAll();
        assertTrue(callCount(callsByTopic, topicA) >= 2);
        assertTrue(callCount(callsByTopic, topicB) >= 2);
    }

    @Test
    public void shouldSerializePatchLoadAndReloadForSameKey() {
        ManualExecutor executor = new ManualExecutor();
        TenantRouteCache cache = newCache(executor);
        NormalMatching existing = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        IMatchedRoutes initial = mockRoutesWithBackingSet(TOPIC, 1, 5, Set.of(existing));
        IMatchedRoutes reloaded = mockRoutesWithBackingSet(TOPIC, 2, 5, Set.of(existing));

        AtomicInteger persistentFanoutCalls = new AtomicInteger();
        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID)))
            .thenAnswer(invocation -> persistentFanoutCalls.getAndIncrement() == 0 ? 1 : 2);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5);

        AtomicInteger matchCalls = new AtomicInteger();
        CountDownLatch reloadStarted = new CountDownLatch(1);
        when(matcher.matchAll(eq(Set.of(TOPIC)), anyInt(), anyInt())).thenAnswer(invocation -> {
            int call = matchCalls.incrementAndGet();
            if (call == 1) {
                return Map.of(TOPIC, initial);
            }
            reloadStarted.countDown();
            return Map.of(TOPIC, reloaded);
        });

        cache.getMatch(TOPIC, FULL_BOUNDARY);
        executor.runNext();

        ticker.advance(FANOUT_CHECK.plusMillis(1));
        cache.getMatch(TOPIC, FULL_BOUNDARY);

        boolean patchLoadDone = false;
        int guard = 10;
        while (guard-- > 0 && reloadStarted.getCount() > 0) {
            executor.runNext();
            if (cache.isCached(TopicUtil.from(TOPIC).getFilterLevelList())) {
                patchLoadDone = true;
            }
        }
        assertTrue(patchLoadDone);
        assertEquals(reloadStarted.getCount(), 0);
    }

    @Test
    public void shouldApplyAddRoutesTask() {
        executorToShutdown = Executors.newSingleThreadExecutor();
        NormalMatching existing = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        IMatchedRoutes matchedRoutes = mockRoutesWithBackingSet(10, 5, Set.of(existing));

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(10);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5);
        when(matcher.matchAll(eq(Set.of(TOPIC)), eq(10), eq(5))).thenReturn(Map.of(TOPIC, matchedRoutes));

        TenantRouteCache cache = newCache(executorToShutdown);

        assertTrue(cache.getMatch(TOPIC, FULL_BOUNDARY).join().contains(existing));

        NormalMatching newNormal = normalMatching(TENANT_ID, TOPIC, 1, "receiverB", "delivererB", 2);
        GroupMatching newGroup = unorderedGroupMatching(TENANT_ID, "sensor/#", "groupA",
            Map.of(receiverUrl(1, "receiverC", "delivererC"), 1L));

        NavigableMap<RouteMatcher, Set<Matching>> additions = new TreeMap<>(Comparators.RouteMatcherComparator);
        additions.put(newNormal.matcher, Set.of(newNormal));
        additions.put(newGroup.matcher, Set.of(newGroup));

        cache.refresh(AddRoutesTask.of(additions));

        await().atMost(Duration.ofSeconds(5)).until(() -> {
            Set<Matching> current = cache.getMatch(TOPIC, FULL_BOUNDARY).join();
            return current.contains(newNormal) && current.contains(newGroup);
        });
    }

    @Test
    public void shouldApplyRemoveRoutesTask() {
        executorToShutdown = Executors.newSingleThreadExecutor();
        TenantRouteCache cache = newCache(executorToShutdown);
        NormalMatching normal = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        GroupMatching removableGroup = unorderedGroupMatching(TENANT_ID, "sensor/#", "groupRemove",
            Map.of(receiverUrl(1, "receiverB", "delivererB"), 1L));
        GroupMatching updatableGroup = unorderedGroupMatching(TENANT_ID, "sensor/+", "groupUpdate",
            Map.of(receiverUrl(1, "receiverC", "delivererC"), 1L,
                receiverUrl(1, "receiverD", "delivererD"), 1L));
        IMatchedRoutes matchedRoutes = mockRoutesWithBackingSet(10, 5, Set.of(normal, removableGroup, updatableGroup));

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(10);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5);
        when(matcher.matchAll(eq(Set.of(TOPIC)), eq(10), eq(5))).thenReturn(Map.of(TOPIC, matchedRoutes));

        cache.getMatch(TOPIC, FULL_BOUNDARY).join();

        GroupMatching emptyGroup = unorderedGroupMatching(TENANT_ID, "sensor/#", "groupRemove", Map.of());
        GroupMatching reducedGroup = unorderedGroupMatching(TENANT_ID, "sensor/+",
            "groupUpdate", Map.of(receiverUrl(1, "receiverC", "delivererC"), 1L));

        NavigableMap<RouteMatcher, Set<Matching>> removals = new TreeMap<>(Comparators.RouteMatcherComparator);
        removals.put(normal.matcher, Set.of(normal));
        removals.put(removableGroup.matcher, Set.of(emptyGroup));
        removals.put(updatableGroup.matcher, Set.of(reducedGroup));

        cache.refresh(RemoveRoutesTask.of(removals));

        await().atMost(Duration.ofSeconds(5)).until(() -> {
            Set<Matching> current = cache.getMatch(TOPIC, FULL_BOUNDARY).join();
            boolean normalRemoved = !current.contains(normal);
            boolean groupRemoved = !current.contains(removableGroup);
            boolean groupUpdated = current.contains(reducedGroup)
                && current.stream().noneMatch(m -> m instanceof GroupMatching gm
                && gm.matcher.equals(updatableGroup.matcher)
                && gm.receivers().equals(updatableGroup.receivers()));
            return normalRemoved && groupRemoved && groupUpdated;
        });
    }

    @Test
    public void shouldQueueTasksUntilLoadCompletes() throws Exception {
        executorToShutdown = Executors.newSingleThreadExecutor();
        TenantRouteCache cache = newCache(executorToShutdown);
        NormalMatching existing = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        IMatchedRoutes matchedRoutes = mockRoutesWithBackingSet(10, 5, Set.of(existing));

        CountDownLatch loadStarted = new CountDownLatch(1);
        CountDownLatch allowLoad = new CountDownLatch(1);

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(10);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5);
        when(matcher.matchAll(eq(Set.of(TOPIC)), eq(10), eq(5))).thenAnswer(invocation -> {
            loadStarted.countDown();
            allowLoad.await(5, TimeUnit.SECONDS);
            return Map.of(TOPIC, matchedRoutes);
        });

        CompletableFuture<Set<Matching>> future = cache.getMatch(TOPIC, FULL_BOUNDARY);
        assertFalse(future.isDone());
        assertTrue(loadStarted.await(5, TimeUnit.SECONDS));

        NormalMatching newNormal = normalMatching(TENANT_ID, TOPIC, 1, "receiverB", "delivererB", 2);
        NavigableMap<RouteMatcher, Set<Matching>> additions = new TreeMap<>(Comparators.RouteMatcherComparator);
        additions.put(newNormal.matcher, Set.of(newNormal));
        cache.refresh(AddRoutesTask.of(additions));

        assertFalse(future.isDone());
        allowLoad.countDown();

        Set<Matching> initial = future.join();
        assertTrue(initial.contains(existing));

        await().atMost(Duration.ofSeconds(5))
            .until(() -> cache.getMatch(TOPIC, FULL_BOUNDARY).join().contains(newNormal));
    }

    @Test
    public void shouldReplayPatchLogDuringLoad() throws Exception {
        executorToShutdown = Executors.newFixedThreadPool(2);
        TenantRouteCache cache = newCache(executorToShutdown);
        String otherTopic = "sensor/humidity";
        NormalMatching existing = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        NormalMatching otherExisting = normalMatching(TENANT_ID, otherTopic, 1, "receiverX", "delivererX", 1);
        IMatchedRoutes matchedRoutes = mockRoutesWithBackingSet(TOPIC, 10, 5, Set.of(existing));
        IMatchedRoutes otherMatchedRoutes = mockRoutesWithBackingSet(otherTopic, 10, 5, Set.of(otherExisting));

        CountDownLatch loadStarted = new CountDownLatch(1);
        CountDownLatch allowLoad = new CountDownLatch(1);

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(10);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5);
        when(matcher.matchAll(any(), eq(10), eq(5))).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Set<String> topics = invocation.getArgument(0);
            String topic = topics.iterator().next();
            if (TOPIC.equals(topic)) {
                loadStarted.countDown();
                if (!allowLoad.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("load timeout");
                }
                return Map.of(TOPIC, matchedRoutes);
            }
            if (otherTopic.equals(topic)) {
                return Map.of(otherTopic, otherMatchedRoutes);
            }
            return Map.of();
        });

        cache.getMatch(otherTopic, FULL_BOUNDARY).join();
        await().atMost(Duration.ofSeconds(5))
            .until(() -> cache.isCached(TopicUtil.from(otherTopic).getFilterLevelList()));

        CompletableFuture<Set<Matching>> future = cache.getMatch(TOPIC, FULL_BOUNDARY);
        assertTrue(loadStarted.await(5, TimeUnit.SECONDS));

        NormalMatching newNormal = normalMatching(TENANT_ID, "sensor/#", 1, "receiverB", "delivererB", 2);
        NavigableMap<RouteMatcher, Set<Matching>> additions = new TreeMap<>(Comparators.RouteMatcherComparator);
        additions.put(newNormal.matcher, Set.of(newNormal));
        cache.refresh(AddRoutesTask.of(additions));

        await().atMost(Duration.ofSeconds(5))
            .until(() -> cache.getMatch(otherTopic, FULL_BOUNDARY).join().contains(newNormal));

        allowLoad.countDown();

        Set<Matching> initial = future.join();
        assertTrue(initial.contains(existing));

        await().atMost(Duration.ofSeconds(5))
            .until(() -> cache.getMatch(TOPIC, FULL_BOUNDARY).join().contains(newNormal));
    }

    @Test
    public void shouldReplayRemoveRoutesDuringLoad() throws Exception {
        executorToShutdown = Executors.newFixedThreadPool(2);
        TenantRouteCache cache = newCache(executorToShutdown);
        String otherTopic = "sensor/humidity";
        NormalMatching existing = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        NormalMatching wildcard = normalMatching(TENANT_ID, "sensor/#", 1, "receiverB", "delivererB", 2);
        IMatchedRoutes matchedRoutes = mockRoutesWithBackingSet(TOPIC, 10, 5, Set.of(existing, wildcard));
        IMatchedRoutes otherMatchedRoutes = mockRoutesWithBackingSet(otherTopic, 10, 5, Set.of(wildcard));

        CountDownLatch loadStarted = new CountDownLatch(1);
        CountDownLatch allowLoad = new CountDownLatch(1);

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(10);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5);
        when(matcher.matchAll(any(), eq(10), eq(5))).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Set<String> topics = invocation.getArgument(0);
            String topic = topics.iterator().next();
            if (TOPIC.equals(topic)) {
                loadStarted.countDown();
                if (!allowLoad.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("load timeout");
                }
                return Map.of(TOPIC, matchedRoutes);
            }
            if (otherTopic.equals(topic)) {
                return Map.of(otherTopic, otherMatchedRoutes);
            }
            return Map.of();
        });

        cache.getMatch(otherTopic, FULL_BOUNDARY).join();
        await().atMost(Duration.ofSeconds(5))
            .until(() -> cache.isCached(TopicUtil.from(otherTopic).getFilterLevelList()));

        CompletableFuture<Set<Matching>> future = cache.getMatch(TOPIC, FULL_BOUNDARY);
        assertTrue(loadStarted.await(5, TimeUnit.SECONDS));

        NavigableMap<RouteMatcher, Set<Matching>> removals = new TreeMap<>(Comparators.RouteMatcherComparator);
        removals.put(wildcard.matcher, Set.of(wildcard));
        cache.refresh(RemoveRoutesTask.of(removals));

        await().atMost(Duration.ofSeconds(5))
            .until(() -> !cache.getMatch(otherTopic, FULL_BOUNDARY).join().contains(wildcard));

        allowLoad.countDown();
        future.join();

        await().atMost(Duration.ofSeconds(5))
            .until(() -> !cache.getMatch(TOPIC, FULL_BOUNDARY).join().contains(wildcard));
    }

    @Test
    public void shouldRetainPatchLogForEarlierLoadAfterLaterLoadCompletes() throws Exception {
        executorToShutdown = Executors.newFixedThreadPool(2);
        TenantRouteCache cache = newCache(executorToShutdown);
        String otherTopic = "sensor/humidity";
        String laterTopic = "sensor/pressure";
        NormalMatching existing = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        NormalMatching wildcard = normalMatching(TENANT_ID, "sensor/#", 1, "receiverB", "delivererB", 2);
        IMatchedRoutes matchedRoutes = mockRoutesWithBackingSet(TOPIC, 10, 5, Set.of(existing, wildcard));
        IMatchedRoutes otherMatchedRoutes = mockRoutesWithBackingSet(otherTopic, 10, 5, Set.of(wildcard));
        IMatchedRoutes laterMatchedRoutes = mockRoutesWithBackingSet(laterTopic, 10, 5, Set.of());

        CountDownLatch loadStarted = new CountDownLatch(1);
        CountDownLatch allowLoad = new CountDownLatch(1);

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(10);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5);
        when(matcher.matchAll(any(), eq(10), eq(5))).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Set<String> topics = invocation.getArgument(0);
            String topic = topics.iterator().next();
            if (TOPIC.equals(topic)) {
                loadStarted.countDown();
                if (!allowLoad.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("load timeout");
                }
                return Map.of(TOPIC, matchedRoutes);
            }
            if (otherTopic.equals(topic)) {
                return Map.of(otherTopic, otherMatchedRoutes);
            }
            if (laterTopic.equals(topic)) {
                return Map.of(laterTopic, laterMatchedRoutes);
            }
            return Map.of();
        });

        cache.getMatch(otherTopic, FULL_BOUNDARY).join();

        CompletableFuture<Set<Matching>> future = cache.getMatch(TOPIC, FULL_BOUNDARY);
        assertTrue(loadStarted.await(5, TimeUnit.SECONDS));

        NavigableMap<RouteMatcher, Set<Matching>> removals = new TreeMap<>(Comparators.RouteMatcherComparator);
        removals.put(wildcard.matcher, Set.of(wildcard));
        cache.refresh(RemoveRoutesTask.of(removals));

        await().atMost(Duration.ofSeconds(5))
            .until(() -> !cache.getMatch(otherTopic, FULL_BOUNDARY).join().contains(wildcard));

        assertFalse(cache.getMatch(laterTopic, FULL_BOUNDARY).join().contains(wildcard));

        allowLoad.countDown();
        future.join();

        await().atMost(Duration.ofSeconds(5))
            .until(() -> !cache.getMatch(TOPIC, FULL_BOUNDARY).join().contains(wildcard));
    }

    @Test
    public void shouldDestroyMeters() {
        TenantRouteCache cache = new TenantRouteCache(RANGE_ID, TENANT_ID, matcher, settingProvider, EXPIRY,
            FANOUT_CHECK,
            directExecutor());
        caches.add(cache);
        assertGauge(TENANT_ID, TenantMetric.MqttRouteCacheSize);

        cache.destroy();
        assertNoGauge(TENANT_ID, TenantMetric.MqttRouteCacheSize);
    }

    @Test
    public void shouldClampPersistentFanoutOnDecreaseNoReload() {
        TenantRouteCache cache = newCache(directExecutor());

        NormalMatching p1 = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
        NormalMatching p2 = normalMatching(TENANT_ID, TOPIC, 1, "receiverB", "delivererB", 2);
        IMatchedRoutes initial = mockRoutesWithBackingSet(2, 5, Set.of(p1, p2));

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(2, 1);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5, 5);
        when(matcher.matchAll(eq(Set.of(TOPIC)), anyInt(), anyInt())).thenReturn(Map.of(TOPIC, initial));

        Set<Matching> first = cache.getMatch(TOPIC, FULL_BOUNDARY).join();
        // Start with 2 persistent routes
        long persistentCount = first.stream().filter(m -> m instanceof NormalMatching nm && nm.subBrokerId() == 1)
            .count();
        assertEquals(persistentCount, 2);

        ticker.advance(FANOUT_CHECK.plusMillis(1));
        Set<Matching> afterClamp = cache.getMatch(TOPIC, FULL_BOUNDARY).join();
        long clampedCount = afterClamp.stream().filter(m -> m instanceof NormalMatching nm && nm.subBrokerId() == 1)
            .count();
        assertEquals(clampedCount, 1);

        // No reload triggered
        verify(matcher, times(1)).matchAll(any(), anyInt(), anyInt());
    }

    @Test
    public void shouldClampGroupFanoutOnDecreaseNoReload() {
        TenantRouteCache cache = newCache(directExecutor());

        GroupMatching g1 = unorderedGroupMatching(TENANT_ID, "sensor/#", "g1",
            Map.of(receiverUrl(1, "r1", "d1"), 1L));
        GroupMatching g2 = unorderedGroupMatching(TENANT_ID, "sensor/#", "g2",
            Map.of(receiverUrl(1, "r2", "d2"), 1L));
        IMatchedRoutes initial = mockRoutesWithBackingSet(10, 2, Set.of(g1, g2));

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(10, 10);
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(2, 1);
        when(matcher.matchAll(eq(Set.of(TOPIC)), anyInt(), anyInt())).thenReturn(Map.of(TOPIC, initial));

        Set<Matching> first = cache.getMatch(TOPIC, FULL_BOUNDARY).join();
        long groupCount = first.stream().filter(m -> m instanceof GroupMatching).count();
        assertEquals(groupCount, 2);

        ticker.advance(FANOUT_CHECK.plusMillis(1));
        Set<Matching> afterClamp = cache.getMatch(TOPIC, FULL_BOUNDARY).join();
        long clampedCount = afterClamp.stream().filter(m -> m instanceof GroupMatching).count();
        assertEquals(clampedCount, 1);

        verify(matcher, times(1)).matchAll(any(), anyInt(), anyInt());
    }

    @Test
    public void shouldBoundZeroRouteTopicsByMaxWeight() {
        // Configure a small max cached routes to validate bounding behavior
        String propKey = DistMaxCachedRoutesPerTenant.INSTANCE.propKey();
        String original = System.getProperty(propKey);
        try {
            System.setProperty(propKey, String.valueOf(10));
            DistMaxCachedRoutesPerTenant.INSTANCE.resolve();

            executorToShutdown = Executors.newSingleThreadExecutor();
            TenantRouteCache cache = newCache(executorToShutdown);

            // All topics have no matching routes (weight should be at least 1 after fix)
            IMatchedRoutes emptyRoutes = mockRoutesWithBackingSet(10, 10, Set.of());
            when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(10);
            when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(10);
            when(matcher.matchAll(any(), anyInt(), anyInt())).thenAnswer(invocation -> {
                @SuppressWarnings("unchecked")
                Set<String> topics = invocation.getArgument(0);
                String t = topics.iterator().next();
                return Map.of(t, emptyRoutes);
            });

            // Access many distinct topics rapidly; cache should stay bounded by max weight
            int total = 100;
            for (int i = 0; i < total; i++) {
                String topic = "sensor/t" + i;
                cache.getMatch(topic, FULL_BOUNDARY).join();
            }

            await().atMost(Duration.ofSeconds(5)).until(() -> {
                int cached = 0;
                for (int i = 0; i < total; i++) {
                    String topic = "sensor/t" + i;
                    if (cache.isCached(TopicUtil.from(topic).getFilterLevelList())) {
                        cached++;
                    }
                }
                return cached > 0 && cached <= 12;
            });

            int cached = 0;
            for (int i = 0; i < total; i++) {
                String topic = "sensor/t" + i;
                if (cache.isCached(TopicUtil.from(topic).getFilterLevelList())) {
                    cached++;
                }
            }
            assertTrue(cached > 0); // Index entries should exist after load
            assertTrue(cached <= 12); // Index entries should be bounded by max weight
        } finally {
            if (original == null) {
                System.clearProperty(propKey);
            } else {
                System.setProperty(propKey, original);
            }
            DistMaxCachedRoutesPerTenant.INSTANCE.resolve();
        }
    }

    @Test
    public void shouldCleanupIndexOnEviction() {
        String propKey = DistMaxCachedRoutesPerTenant.INSTANCE.propKey();
        String original = System.getProperty(propKey);
        try {
            System.setProperty(propKey, String.valueOf(1));
            DistMaxCachedRoutesPerTenant.INSTANCE.resolve();

            executorToShutdown = Executors.newSingleThreadExecutor();
            TenantRouteCache cache = newCache(executorToShutdown);

            String otherTopic = "sensor/humidity";
            NormalMatching existing = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
            NormalMatching otherExisting = normalMatching(TENANT_ID, otherTopic, 1, "receiverB", "delivererB", 1);
            IMatchedRoutes matchedRoutes = mockRoutesWithBackingSet(TOPIC, 10, 5, Set.of(existing));
            IMatchedRoutes otherMatchedRoutes = mockRoutesWithBackingSet(otherTopic, 10, 5, Set.of(otherExisting));

            when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(10);
            when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5);
            when(matcher.matchAll(any(), eq(10), eq(5))).thenAnswer(invocation -> {
                @SuppressWarnings("unchecked")
                Set<String> topics = invocation.getArgument(0);
                String topic = topics.iterator().next();
                if (TOPIC.equals(topic)) {
                    return Map.of(TOPIC, matchedRoutes);
                }
                if (otherTopic.equals(topic)) {
                    return Map.of(otherTopic, otherMatchedRoutes);
                }
                return Map.of();
            });

            assertTrue(cache.getMatch(TOPIC, FULL_BOUNDARY).join().contains(existing));
            await().atMost(Duration.ofSeconds(5))
                .until(() -> cache.isCached(TopicUtil.from(TOPIC).getFilterLevelList()));

            assertTrue(cache.getMatch(otherTopic, FULL_BOUNDARY).join().contains(otherExisting));
            await().atMost(Duration.ofSeconds(5)).until(() -> {
                boolean firstCached = cache.isCached(TopicUtil.from(TOPIC).getFilterLevelList());
                boolean secondCached = cache.isCached(TopicUtil.from(otherTopic).getFilterLevelList());
                return !firstCached && secondCached;
            });
        } finally {
            if (original == null) {
                System.clearProperty(propKey);
            } else {
                System.setProperty(propKey, original);
            }
            DistMaxCachedRoutesPerTenant.INSTANCE.resolve();
        }
    }

    @Test
    public void shouldSkipIndexingWhenEvictedBeforePatchLoad() {
        String propKey = DistMaxCachedRoutesPerTenant.INSTANCE.propKey();
        String original = System.getProperty(propKey);
        try {
            System.setProperty(propKey, String.valueOf(1));
            DistMaxCachedRoutesPerTenant.INSTANCE.resolve();

            ManualExecutor executor = new ManualExecutor();
            TenantRouteCache cache = newCache(executor);

            String otherTopic = "sensor/humidity";
            NormalMatching existing = normalMatching(TENANT_ID, TOPIC, 1, "receiverA", "delivererA", 1);
            NormalMatching otherExisting = normalMatching(TENANT_ID, otherTopic, 1, "receiverB", "delivererB", 1);
            IMatchedRoutes matchedRoutes = mockRoutesWithBackingSet(TOPIC, 10, 5, Set.of(existing));
            IMatchedRoutes otherMatchedRoutes = mockRoutesWithBackingSet(otherTopic, 10, 5, Set.of(otherExisting));

            when(settingProvider.provide(eq(Setting.MaxPersistentFanout), eq(TENANT_ID))).thenReturn(10);
            when(settingProvider.provide(eq(Setting.MaxGroupFanout), eq(TENANT_ID))).thenReturn(5);
            when(matcher.matchAll(any(), eq(10), eq(5))).thenAnswer(invocation -> {
                @SuppressWarnings("unchecked")
                Set<String> topics = invocation.getArgument(0);
                String topic = topics.iterator().next();
                if (TOPIC.equals(topic)) {
                    return Map.of(TOPIC, matchedRoutes);
                }
                if (otherTopic.equals(topic)) {
                    return Map.of(otherTopic, otherMatchedRoutes);
                }
                return Map.of();
            });

            CompletableFuture<Set<Matching>> first = cache.getMatch(TOPIC, FULL_BOUNDARY);
            CompletableFuture<Set<Matching>> second = cache.getMatch(otherTopic, FULL_BOUNDARY);

            executor.runNext();
            executor.runNext();
            executor.runAll();

            first.join();
            second.join();

            assertFalse(cache.isCached(TopicUtil.from(TOPIC).getFilterLevelList()));
            assertTrue(cache.isCached(TopicUtil.from(otherTopic).getFilterLevelList()));
        } finally {
            if (original == null) {
                System.clearProperty(propKey);
            } else {
                System.setProperty(propKey, original);
            }
            DistMaxCachedRoutesPerTenant.INSTANCE.resolve();
        }
    }

    private TenantRouteCache newCache(Executor executor) {
        TenantRouteCache cache = new TenantRouteCache(RANGE_ID, TENANT_ID, matcher, settingProvider,
            EXPIRY, FANOUT_CHECK, ticker, executor);
        caches.add(cache);
        return cache;
    }

    private int callCount(Map<String, AtomicInteger> callsByTopic, String topic) {
        AtomicInteger counter = callsByTopic.get(topic);
        return counter == null ? 0 : counter.get();
    }

    private IMatchedRoutes mockRoutesWithBackingSet(int maxPersistentFanout, int maxGroupFanout, Set<Matching> seed) {
        return mockRoutesWithBackingSet(TOPIC, maxPersistentFanout, maxGroupFanout, seed);
    }

    private IMatchedRoutes mockRoutesWithBackingSet(String topic,
                                                    int maxPersistentFanout,
                                                    int maxGroupFanout,
                                                    Set<Matching> seed) {
        MatchedRoutes routes = new MatchedRoutes(TENANT_ID, topic, eventCollector, maxPersistentFanout,
            maxGroupFanout);
        for (Matching m : seed) {
            if (m instanceof NormalMatching nm) {
                routes.addNormalMatching(nm);
            } else if (m instanceof GroupMatching gm) {
                routes.putGroupMatching(gm);
            }
        }
        return routes;
    }

    private static final class ManualTicker implements Ticker {
        private final AtomicLong nanos = new AtomicLong();

        @Override
        public long read() {
            return nanos.get();
        }

        void advance(Duration duration) {
            nanos.addAndGet(duration.toNanos());
        }
    }

    private static final class ManualExecutor implements Executor {
        private final Deque<Runnable> tasks = new ArrayDeque<>();

        @Override
        public void execute(Runnable command) {
            tasks.addLast(command);
        }

        int pending() {
            return tasks.size();
        }

        void runNext() {
            Runnable task = tasks.pollFirst();
            if (task != null) {
                task.run();
            }
        }

        void runAll() {
            while (!tasks.isEmpty()) {
                runNext();
            }
        }
    }
}
