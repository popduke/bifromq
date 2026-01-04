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

import static org.apache.bifromq.util.TopicConst.MULTI_WILDCARD;
import static org.apache.bifromq.util.TopicConst.SINGLE_WILDCARD;
import static org.apache.bifromq.util.TopicConst.SYS_PREFIX;

import java.util.List;
import org.apache.bifromq.type.RouteMatcher;

public final class TopicMatcher {
    private final List<String> topicLevels;
    private final String topic;

    public TopicMatcher(String topic) {
        this.topic = topic;
        topicLevels = TopicUtil.parse(topic, false);
    }

    public boolean match(RouteMatcher matcher) {
        if (matcher.getType() == RouteMatcher.Type.Normal) {
            if (topic.equals(matcher.getMqttTopicFilter())) {
                return true;
            }
        }
        return matchLevels(matcher.getFilterLevelList(), topicLevels);
    }

    private boolean matchLevels(List<String> filterLevels, List<String> levels) {
        int topicLevelCount = levels.size();
        for (int filterLevelIndex = 0; filterLevelIndex < filterLevels.size(); filterLevelIndex++) {
            String filterLevel = filterLevels.get(filterLevelIndex);
            boolean sysTopic = !levels.isEmpty() && levels.get(0).startsWith(SYS_PREFIX);
            if (MULTI_WILDCARD.equals(filterLevel)) {
                return filterLevelIndex != 0 || !sysTopic;
            }
            if (filterLevelIndex >= topicLevelCount) {
                return false;
            }
            if (SINGLE_WILDCARD.equals(filterLevel)) {
                if (filterLevelIndex == 0 && sysTopic) {
                    return false;
                }
            } else if (!filterLevel.equals(levels.get(filterLevelIndex))) {
                return false;
            }
        }
        return topicLevelCount == filterLevels.size();
    }
}
