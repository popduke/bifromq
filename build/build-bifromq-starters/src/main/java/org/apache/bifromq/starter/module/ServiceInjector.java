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

package org.apache.bifromq.starter.module;

import com.google.inject.Injector;
import com.google.inject.Key;

/**
 * This class is a wrapper of Guice Injector to walk around the limitation of Guice Injector is unbindable.
 */
public class ServiceInjector {
    private final Injector delegate;

    public ServiceInjector(com.google.inject.Injector delegate) {
        this.delegate = delegate;
    }

    public <T> T getInstance(Key<T> key) {
        return delegate.getInstance(key);
    }

    public <T> T getInstance(Class<T> type) {
        return delegate.getInstance(type);
    }
}
