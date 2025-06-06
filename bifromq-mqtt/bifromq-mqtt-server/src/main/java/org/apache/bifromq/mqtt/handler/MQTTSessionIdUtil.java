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

package org.apache.bifromq.mqtt.handler;

import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CHANNEL_ID_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;

import org.apache.bifromq.type.ClientInfo;

public class MQTTSessionIdUtil {
    public static int packetId(long seq) {
        return (int) (seq % 65535) + 1;
    }

    public static String userSessionId(ClientInfo info) {
        String requestClientId = info.getMetadataOrDefault(MQTT_CLIENT_ID_KEY, "");
        if (requestClientId.isEmpty()) {
            // if client id is assigned by server
            return info.getMetadataOrThrow(MQTT_USER_ID_KEY) + "/" + info.getMetadataOrDefault(MQTT_CHANNEL_ID_KEY, "");
        }
        return info.getMetadataOrThrow(MQTT_USER_ID_KEY) + "/" + requestClientId;
    }
}
