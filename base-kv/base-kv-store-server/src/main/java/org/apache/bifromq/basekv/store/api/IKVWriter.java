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

package org.apache.bifromq.basekv.store.api;

import com.google.protobuf.ByteString;
import org.apache.bifromq.basekv.proto.Boundary;

public interface IKVWriter {

    void reset();

    void delete(ByteString key);

    void clear(Boundary boundary);

    /**
     * Insert a non-exist key value pair, if the key is already exist, the result is undefined.
     *
     * @param key
     * @param value
     */
    void insert(ByteString key, ByteString value);

    /**
     * Put a key value pair, if the key is existed, its value will be overridden.
     *
     * @param key
     * @param value
     */
    void put(ByteString key, ByteString value);
}
