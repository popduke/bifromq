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

package org.apache.bifromq.basekv.store;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.fail;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.MockableTest;
import org.apache.bifromq.basekv.annotation.Cluster;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.option.KVRangeOptions;
import org.apache.bifromq.basekv.store.option.KVRangeStoreOptions;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;

@Slf4j
public abstract class KVRangeStoreClusterTestTemplate extends MockableTest {
    protected KVRangeStoreTestCluster cluster;
    private int initVoters = 3;
    private int initLearners = 0;
    private KVRangeStoreOptions options;

    private void createClusterByAnnotation(Method testMethod) {
        Cluster cluster = testMethod.getAnnotation(Cluster.class);
        options = new KVRangeStoreOptions();
        if (cluster != null) {
            Preconditions.checkArgument(cluster.initVoters() > 0,
                "Init nodes number must be greater than zero");
            initVoters = cluster.initVoters();
            initLearners = cluster.initLearners();
            KVRangeOptions rangeOptions = new KVRangeOptions();
            rangeOptions.setWalRaftConfig(rangeOptions.getWalRaftConfig().setAsyncAppend(cluster.asyncAppend()));
            rangeOptions.setWalRaftConfig(rangeOptions.getWalRaftConfig()
                .setInstallSnapshotTimeoutTick(cluster.installSnapshotTimeoutTick()));
            // Deterministic timeouts configurable via annotation
            rangeOptions.setSnapshotSyncIdleTimeoutSec(cluster.snapshotSyncIdleTimeoutSec());
            rangeOptions.setZombieTimeoutSec(cluster.zombieTimeoutSec());
            rangeOptions.setMergeTimeoutSec(cluster.mergeTimeoutSec());
            options.setKvRangeOptions(rangeOptions);
        } else {
            initVoters = 3;
            initLearners = 0;
            KVRangeOptions rangeOptions = options.getKvRangeOptions();
            // Speed up snapshot/data-merge idle timeout for deterministic tests
            rangeOptions.setSnapshotSyncIdleTimeoutSec(5);
            rangeOptions.setZombieTimeoutSec(5);
            rangeOptions.setMergeTimeoutSec(5);
        }
    }

    @Override
    protected void doSetup(Method method) {
        try {
            RxJavaPlugins.setErrorHandler(e -> log.debug("Unhandled exception", e));
            createClusterByAnnotation(method);
            log.info("Starting test cluster");
            cluster = new KVRangeStoreTestCluster(options);
            String store0 = cluster.bootstrapStore();
            KVRangeId rangeId = cluster.genesisKVRangeId();
            cluster.awaitKVRangeReady(store0, rangeId);

            Set<String> voters = Sets.newHashSet(store0);
            for (int i = 1; i < initVoters; i++) {
                voters.add(cluster.addStore());
            }
            Set<String> learners = Sets.newHashSet();
            for (int i = 0; i < initLearners; i++) {
                learners.add(cluster.addStore());
            }
            Set<String> allStores = new HashSet<>(voters);
            allStores.addAll(learners);
            long start = System.currentTimeMillis();
            log.info("Preparing replica config for testing: voters={}, learners={}", voters, learners);
            await().ignoreExceptions().atMost(120, TimeUnit.SECONDS).until(() -> {
                KVRangeConfig setting = cluster.kvRangeSetting(rangeId);
                String leader = setting.leader;
                if (!setting.clusterConfig.getVotersList().containsAll(voters)
                    || !setting.clusterConfig.getLearnersList().containsAll(learners)) {
                    try {
                        cluster.changeReplicaConfig(leader, setting.ver, rangeId, voters, learners)
                            .toCompletableFuture().join();
                        await().atMost(30, TimeUnit.SECONDS).until(() -> {
                            for (String store : allStores) {
                                KVRangeDescriptor rangeDesc = cluster.getKVRange(store, rangeId);
                                if (rangeDesc == null) {
                                    return false;
                                }
                                if (!rangeDesc.getConfig().getVotersList().containsAll(voters)
                                    || !rangeDesc.getConfig().getLearnersList().containsAll(learners)) {
                                    return false;
                                }
                            }
                            return true;
                        });
                        return true;
                    } catch (Throwable e) {
                        return false;
                    }
                }
                return true;
            });
            log.info("KVRange[{}] ready in {}ms start testing", KVRangeIdUtil.toString(rangeId),
                System.currentTimeMillis() - start);
        } catch (Throwable e) {
            log.error("Failed to setup test cluster", e);
            fail();
        }
    }

    @Override
    protected void doTearDown(Method method) {
        if (cluster != null) {
            log.info("Shutting down test cluster");
            KVRangeStoreTestCluster lastCluster = this.cluster;
            lastCluster.shutdown();
        }
        RxJavaPlugins.setErrorHandler(null);
    }

    public String nonLeaderStore(KVRangeConfig setting) {
        Set<String> followerStores = followStores(setting);
        if (followerStores.isEmpty()) {
            throw new RuntimeException("No non-leader store");
        }
        return followerStores.iterator().next();
    }

    public Set<String> followStores(KVRangeConfig setting) {
        Set<String> voters = new HashSet<>(setting.clusterConfig.getVotersList());
        voters.remove(setting.leader);
        return voters;
    }
}
