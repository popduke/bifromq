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

package org.apache.bifromq.util;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.type.RouteMatcher;
import org.testng.annotations.Test;

public class TopicMatcherTest {
    @Test
    public void shouldMatchExactNormal() {
        TopicMatcher matcher = new TopicMatcher("sensor/temperature");
        RouteMatcher filter = TopicUtil.from("sensor/temperature");
        assertTrue(matcher.match(filter));
    }

    @Test
    public void shouldMatchWildcardNormal() {
        TopicMatcher matcher = new TopicMatcher("sensor/temperature");
        RouteMatcher filter = TopicUtil.from("sensor/+");
        assertTrue(matcher.match(filter));
        assertFalse(matcher.match(TopicUtil.from("device/+")));
    }

    @Test
    public void shouldRejectSysTopicByRootWildcard() {
        TopicMatcher matcher = new TopicMatcher("$sys/a/b");
        assertFalse(matcher.match(TopicUtil.from("#")));
        assertFalse(matcher.match(TopicUtil.from("+")));
        assertFalse(matcher.match(TopicUtil.from("+/+/+")));
    }

    @Test
    public void shouldMatchSharedSubscription() {
        TopicMatcher matcher = new TopicMatcher("sensor/temperature");
        RouteMatcher unordered = TopicUtil.from("$share/groupA/sensor/+");
        RouteMatcher ordered = TopicUtil.from("$oshare/groupB/sensor/+");
        assertTrue(matcher.match(unordered));
        assertTrue(matcher.match(ordered));
    }

    @Test
    public void shouldMatchMultiWildcardAtRoot() {
        TopicMatcher matcher = new TopicMatcher("a");
        assertTrue(matcher.match(TopicUtil.from("#")));
        assertTrue(matcher.match(TopicUtil.from("a/#")));
        assertTrue(matcher.match(TopicUtil.from("+/#")));
    }

    @Test
    public void shouldMatchMultiWildcardAcrossLevels() {
        TopicMatcher matcher = new TopicMatcher("a/b/c");
        assertTrue(matcher.match(TopicUtil.from("a/#")));
        assertTrue(matcher.match(TopicUtil.from("+/#")));
        assertFalse(matcher.match(TopicUtil.from("b/#")));
    }

    @Test
    public void shouldMatchSingleWildcardPerLevel() {
        TopicMatcher matcher = new TopicMatcher("a/b");
        assertTrue(matcher.match(TopicUtil.from("a/+")));
        assertFalse(matcher.match(TopicUtil.from("a/+/+")));
        assertFalse(matcher.match(TopicUtil.from("a")));
    }

    @Test
    public void shouldHandleLeadingDelimiterTopic() {
        TopicMatcher matcher = new TopicMatcher("/a");
        assertTrue(matcher.match(TopicUtil.from("#")));
        assertTrue(matcher.match(TopicUtil.from("+/#")));
        assertTrue(matcher.match(TopicUtil.from("/#")));
        assertTrue(matcher.match(TopicUtil.from("/+")));
        assertFalse(matcher.match(TopicUtil.from("+")));
    }

    @Test
    public void shouldHandleTrailingDelimiterTopic() {
        TopicMatcher matcher = new TopicMatcher("a/");
        assertTrue(matcher.match(TopicUtil.from("a/+")));
        assertTrue(matcher.match(TopicUtil.from("a/#")));
        assertFalse(matcher.match(TopicUtil.from("a")));
    }

    @Test
    public void shouldMatchSysTopicWithExplicitPrefix() {
        TopicMatcher matcher = new TopicMatcher("$sys/a/b");
        assertTrue(matcher.match(TopicUtil.from("$sys/#")));
        assertFalse(matcher.match(TopicUtil.from("#")));
        assertFalse(matcher.match(TopicUtil.from("+")));
    }
}
