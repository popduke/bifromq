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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.bifromq.dist.worker.schema.cache.Matching;
import org.apache.bifromq.type.RouteMatcher;

final class PatchLog {
    private final List<Record> records = new ArrayList<>();
    private long startSeq;

    boolean isEmpty() {
        return records.isEmpty();
    }

    void clear() {
        records.clear();
        startSeq = 0;
    }

    void append(long seq, Map<RouteMatcher, Set<Matching>> routes, boolean isAdd) {
        if (records.isEmpty()) {
            startSeq = seq;
        }
        records.add(new Record(routes, isAdd));
    }

    void forEach(long startSeq, long endSeq, Consumer<Record> consumer) {
        if (records.isEmpty() || startSeq >= endSeq) {
            return;
        }
        long fromSeq = Math.max(startSeq + 1, this.startSeq);
        long toSeq = Math.min(endSeq, this.startSeq + records.size() - 1);
        if (fromSeq > toSeq) {
            return;
        }
        int fromIndex = (int) (fromSeq - this.startSeq);
        int toIndex = (int) (toSeq - this.startSeq);
        for (int i = fromIndex; i <= toIndex; i++) {
            consumer.accept(records.get(i));
        }
    }

    void trimBefore(long trimToSeq) {
        if (records.isEmpty() || trimToSeq <= startSeq) {
            return;
        }
        int trimCount = (int) Math.min(records.size(), trimToSeq - startSeq);
        records.subList(0, trimCount).clear();
        startSeq += trimCount;
        if (records.isEmpty()) {
            startSeq = 0;
        }
    }

    static final class Record {
        final Map<RouteMatcher, Set<Matching>> routes;
        final boolean isAdd;

        private Record(Map<RouteMatcher, Set<Matching>> routes, boolean isAdd) {
            this.routes = routes;
            this.isAdd = isAdd;
        }
    }
}
