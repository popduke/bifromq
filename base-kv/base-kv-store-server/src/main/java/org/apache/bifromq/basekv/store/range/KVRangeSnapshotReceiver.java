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

package org.apache.bifromq.basekv.store.range;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.observers.DisposableObserver;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bifromq.base.util.CascadeCancelCompletableFuture;
import org.apache.bifromq.basekv.proto.KVPair;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeMessage;
import org.apache.bifromq.basekv.proto.SaveSnapshotDataReply;
import org.apache.bifromq.basekv.proto.SaveSnapshotDataRequest;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.slf4j.Logger;

class KVRangeSnapshotReceiver implements IKVRangeSnapshotReceiver {
    private final Logger log;
    private final String sessionId;
    private final IKVRangeMessenger messenger;
    private final IKVRangeMetricManager metricManager;
    private final Executor executor;
    private final int idleTimeSec;
    private final KVRangeId sourceRangeId;
    private final String sourceStoreId;

    public KVRangeSnapshotReceiver(String sessionId,
                                   KVRangeId sourceRangeId,
                                   String sourceStoreId,
                                   IKVRangeMessenger messenger,
                                   IKVRangeMetricManager metricManager,
                                   Executor executor,
                                   int idleTimeSec,
                                   Logger log) {
        this.sessionId = sessionId;
        this.sourceRangeId = sourceRangeId;
        this.sourceStoreId = sourceStoreId;
        this.messenger = messenger;
        this.metricManager = metricManager;
        this.executor = executor;
        this.idleTimeSec = idleTimeSec;
        this.log = log;
    }

    @Override
    public CompletableFuture<Result> start(IKVRangeDataSetter dataSetter) {
        CompletableFuture<Result> onDone = new CompletableFuture<>();
        try {
            DisposableObserver<KVRangeMessage> observer = messenger.receive()
                .filter(m -> m.hasSaveSnapshotDataRequest()
                    && m.getHostStoreId().equals(sourceStoreId)
                    && m.getSaveSnapshotDataRequest().getSessionId().equals(sessionId))
                .timeout(idleTimeSec, TimeUnit.SECONDS, Schedulers.from(executor))
                .observeOn(Schedulers.from(executor))
                .subscribeWith(new DisposableObserver<KVRangeMessage>() {
                    @Override
                    public void onNext(@NonNull KVRangeMessage m) {
                        SaveSnapshotDataRequest request = m.getSaveSnapshotDataRequest();
                        try {
                            switch (request.getFlag()) {
                                case More, End -> {
                                    int bytes = 0;
                                    for (KVPair kv : request.getKvList()) {
                                        bytes += kv.getKey().size();
                                        bytes += kv.getValue().size();
                                        dataSetter.put(kv.getKey(), kv.getValue());
                                    }
                                    metricManager.reportRestore(bytes);
                                    log.debug("Received {} bytes: rangeId={}, storeId={}, session={}",
                                        bytes, KVRangeIdUtil.toString(sourceRangeId), sourceStoreId, sessionId);
                                    if (request.getFlag() == SaveSnapshotDataRequest.Flag.End) {
                                        if (!onDone.isCancelled()) {
                                            dispose();
                                            onDone.complete(Result.DONE);
                                            log.debug("Finish data receiving: rangeId={}, storeId={}, session={}",
                                                KVRangeIdUtil.toString(sourceRangeId), sourceStoreId, sessionId);
                                        } else {
                                            dataSetter.reset();
                                            dispose();
                                            log.debug("Receiver canceled: session={}", sessionId);
                                        }
                                    }
                                    log.debug("Send reply: rangeId={}, storeId={}, session={}",
                                        KVRangeIdUtil.toString(sourceRangeId), sourceStoreId, sessionId);
                                    messenger.send(KVRangeMessage.newBuilder()
                                        .setRangeId(sourceRangeId)
                                        .setHostStoreId(sourceStoreId)
                                        .setSaveSnapshotDataReply(SaveSnapshotDataReply.newBuilder()
                                            .setReqId(request.getReqId())
                                            .setSessionId(request.getSessionId())
                                            .setResult(SaveSnapshotDataReply.Result.OK)
                                            .build())
                                        .build());
                                }
                                case NotFound -> {
                                    dataSetter.reset();
                                    onDone.complete(Result.NOT_FOUND);
                                    dispose();
                                }
                                default -> {
                                    log.debug("Failed to receive data: rangeId={}, storeId={}, session={}",
                                        KVRangeIdUtil.toString(sourceRangeId), sourceStoreId, sessionId);
                                    dataSetter.reset();
                                    onDone.complete(Result.ERROR);
                                    dispose();
                                }
                            }
                        } catch (Throwable t) {
                            onError(t);
                            messenger.send(KVRangeMessage.newBuilder()
                                .setRangeId(sourceRangeId)
                                .setHostStoreId(sourceStoreId)
                                .setSaveSnapshotDataReply(SaveSnapshotDataReply.newBuilder()
                                    .setSessionId(sessionId)
                                    .setResult(SaveSnapshotDataReply.Result.Error)
                                    .build())
                                .build());
                        }
                    }

                    @Override
                    public void onError(@NonNull Throwable e) {
                        log.error("Receiving data error: rangeId={}, storeId={}, session={}",
                            KVRangeIdUtil.toString(sourceRangeId), sourceStoreId, sessionId, e);
                        dataSetter.reset();
                        if (e instanceof TimeoutException) {
                            onDone.complete(Result.TIME_OUT);
                        } else {
                            onDone.complete(Result.ERROR);
                        }
                        dispose();
                    }

                    @Override
                    public void onComplete() {

                    }
                });
            onDone.whenCompleteAsync((v, e) -> {
                if (onDone.isCancelled()) {
                    observer.dispose();
                    dataSetter.reset();
                    messenger.send(KVRangeMessage.newBuilder()
                        .setRangeId(sourceRangeId)
                        .setHostStoreId(sourceStoreId)
                        .setSaveSnapshotDataReply(SaveSnapshotDataReply.newBuilder()
                            .setSessionId(sessionId)
                            .setResult(SaveSnapshotDataReply.Result.NoSessionFound)
                            .build())
                        .build());
                }
            }, executor);
        } catch (Throwable t) {
            log.error("Unexpected error", t);
            onDone.completeExceptionally(t);
        }
        return CascadeCancelCompletableFuture.fromRoot(onDone);
    }
}
