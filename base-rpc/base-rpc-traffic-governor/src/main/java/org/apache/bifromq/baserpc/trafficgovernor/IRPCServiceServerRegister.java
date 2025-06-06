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

package org.apache.bifromq.baserpc.trafficgovernor;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * The interface for the registration of a server into a service.
 */
public interface IRPCServiceServerRegister {
    /**
     * The registration of a server into a service.
     */
    interface IServerRegistration {
        void stop();
    }

    /**
     * Join the registration by join a server into the service.
     *
     * @param id        the id of the server
     * @param hostAddr  the hosting address
     * @param groupTags the initial group tags assigned
     * @param attrs     the associated attributes
     */
    IServerRegistration reg(String id, InetSocketAddress hostAddr, Set<String> groupTags, Map<String, String> attrs);

    default IServerRegistration reg(String id, InetSocketAddress hostAddr) {
        return reg(id, hostAddr, Collections.emptySet(), Collections.emptyMap());
    }
}
