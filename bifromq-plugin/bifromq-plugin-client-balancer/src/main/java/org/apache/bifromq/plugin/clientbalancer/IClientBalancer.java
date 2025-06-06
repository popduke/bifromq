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

package org.apache.bifromq.plugin.clientbalancer;

import org.apache.bifromq.type.ClientInfo;
import java.util.Optional;
import org.pf4j.ExtensionPoint;

/**
 * Plugin interface for balancing mqtt client in the cluster.
 */
public interface IClientBalancer extends ExtensionPoint {

    /**
     * Check if the client needs to be redirected. Implementation must be lightweight/non-blocking.
     *
     * @param clientInfo the client to be checked
     * @return redirection hint
     */
    Optional<Redirection> needRedirect(ClientInfo clientInfo);

    /**
     * Close the client balancer.
     */
    default void close() {

    }
}
