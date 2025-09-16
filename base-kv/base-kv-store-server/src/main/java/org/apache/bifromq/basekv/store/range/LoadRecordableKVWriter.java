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

import com.google.protobuf.ByteString;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.store.api.IKVWriter;

class LoadRecordableKVWriter implements IKVWriter {
    private final IKVWriter delegate;
    private final IKVLoadRecorder recorder;

    public LoadRecordableKVWriter(IKVWriter delegate, IKVLoadRecorder recorder) {
        this.delegate = delegate;
        this.recorder = recorder;
    }

    @Override
    public void reset() {
        long start = System.nanoTime();
        delegate.reset();
        recorder.record(System.nanoTime() - start);
    }

    @Override
    public void delete(ByteString key) {
        long start = System.nanoTime();
        delegate.delete(key);
        recorder.record(key, System.nanoTime() - start);
    }

    @Override
    public void clear(Boundary boundary) {
        long start = System.nanoTime();
        delegate.clear(boundary);
        recorder.record(System.nanoTime() - start);
    }

    @Override
    public void insert(ByteString key, ByteString value) {
        long start = System.nanoTime();
        delegate.insert(key, value);
        recorder.record(key, System.nanoTime() - start);
    }

    @Override
    public void put(ByteString key, ByteString value) {
        long start = System.nanoTime();
        delegate.put(key, value);
        recorder.record(key, System.nanoTime() - start);
    }
}
