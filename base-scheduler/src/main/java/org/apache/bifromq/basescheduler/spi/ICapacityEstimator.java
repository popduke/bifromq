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

package org.apache.bifromq.basescheduler.spi;

/**
 * SPI interface for estimating downstream capacity.
 */
public interface ICapacityEstimator<BatcherKey> {
    /**
     * Record the outcome of a batch call.
     *
     * @param weight the total weight of the batch
     * @param latencyNs    the execution latency in nanoseconds
     */
    void record(long weight, long latencyNs);

    /**
     * Determine if it's allowed to emit batch call for given batcher.
     *
     * @param inflightWeight the inflight weight of batch call
     * @param batcherKey the key of the batcher
     * @return if it's allowed
     */
    boolean hasCapacity(long inflightWeight, BatcherKey batcherKey);

    /**
     * Get the current maximum allowed capacity in weighted size for given batcher.
     *
     * @param batcherKey the key of the batcher
     * @return the capacity budget
     */
    long maxCapacity(BatcherKey batcherKey);

    /**
     * Notify estimator that downstream backpressure has been observed.
     */
    void onBackPressure();
}
