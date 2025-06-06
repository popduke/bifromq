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

package org.apache.bifromq.sysprops.parser;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class BooleanParser implements PropParser<Boolean> {
    public static final BooleanParser INSTANCE = new BooleanParser();

    @Override
    public Boolean parse(String value) {
        if ("true".equalsIgnoreCase(value)
            || "y".equalsIgnoreCase(value)
            || "yes".equalsIgnoreCase(value)
            || "1".equals(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)
            || "n".equalsIgnoreCase(value)
            || "no".equalsIgnoreCase(value)
            || "0".equals(value)) {
            return false;
        }
        throw new SysPropParseException(String.format("Unable to parse %s to boolean", value));
    }
}
