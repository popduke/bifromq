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

package org.apache.bifromq.plugin.eventcollector;

import org.apache.bifromq.type.ClientInfo;
import lombok.ToString;

@ToString(callSuper = true)
public abstract class ClientEvent<T extends ClientEvent<T>> extends Event<T> {
    private ClientInfo clientInfo;

    public final ClientInfo clientInfo() {
        return this.clientInfo;
    }

    public final T clientInfo(ClientInfo clientInfo) {
        this.clientInfo = clientInfo;
        return (T) this;
    }

    @Override
    public void clone(T orig) {
        super.clone(orig);
        this.clientInfo = orig.clientInfo();
    }
}
