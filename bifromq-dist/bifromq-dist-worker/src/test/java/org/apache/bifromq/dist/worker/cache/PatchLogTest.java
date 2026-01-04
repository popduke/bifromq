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

import static org.apache.bifromq.dist.worker.schema.cache.Matchings.normalMatching;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.bifromq.dist.worker.schema.cache.Matching;
import org.apache.bifromq.dist.worker.schema.cache.NormalMatching;
import org.apache.bifromq.type.RouteMatcher;
import org.testng.annotations.Test;

public class PatchLogTest {
    private static final String TENANT_ID = "tenantA";

    @Test
    public void shouldIterateRangeInOrder() {
        PatchLog log = new PatchLog();
        NormalMatching first = normalMatching(TENANT_ID, "t1", 1, "receiver1", "deliverer1", 1);
        NormalMatching second = normalMatching(TENANT_ID, "t2", 1, "receiver2", "deliverer2", 1);
        NormalMatching third = normalMatching(TENANT_ID, "t3", 1, "receiver3", "deliverer3", 1);

        log.append(5, routesOf(first), true);
        log.append(6, routesOf(second), false);
        log.append(7, routesOf(third), true);

        List<Boolean> flags = new ArrayList<>();
        log.forEach(4, 7, record -> flags.add(record.isAdd));
        assertEquals(flags, List.of(true, false, true));

        flags.clear();
        log.forEach(5, 6, record -> flags.add(record.isAdd));
        assertEquals(flags, List.of(false));
    }

    @Test
    public void shouldTrimBeforeSequence() {
        PatchLog log = new PatchLog();
        NormalMatching first = normalMatching(TENANT_ID, "a", 1, "receiver1", "deliverer1", 1);
        NormalMatching second = normalMatching(TENANT_ID, "b", 1, "receiver2", "deliverer2", 1);
        NormalMatching third = normalMatching(TENANT_ID, "c", 1, "receiver3", "deliverer3", 1);

        log.append(10, routesOf(first), true);
        log.append(11, routesOf(second), false);
        log.append(12, routesOf(third), true);

        log.trimBefore(12);
        List<Boolean> flags = new ArrayList<>();
        log.forEach(9, 12, record -> flags.add(record.isAdd));
        assertEquals(flags, List.of(true));

        log.trimBefore(20);
        flags.clear();
        log.forEach(0, 30, record -> flags.add(record.isAdd));
        assertTrue(flags.isEmpty());

        log.append(30, routesOf(first), false);
        flags.clear();
        log.forEach(29, 30, record -> flags.add(record.isAdd));
        assertEquals(flags, List.of(false));
    }

    private Map<RouteMatcher, Set<Matching>> routesOf(NormalMatching matching) {
        return Map.of(matching.matcher, Set.of(matching));
    }
}
