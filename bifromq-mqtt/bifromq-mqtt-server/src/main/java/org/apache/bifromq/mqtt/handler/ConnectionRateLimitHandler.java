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

import static org.apache.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;

import com.google.common.util.concurrent.RateLimiter;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.util.ReferenceCountUtil;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.channelclosed.ChannelError;

@Slf4j
@ChannelHandler.Sharable
public class ConnectionRateLimitHandler extends ChannelDuplexHandler {
    private final RateLimiter rateLimiter;
    private final IEventCollector eventCollector;
    private final ChannelPipelineInitializer initializer;
    private boolean accepted = false;

    public ConnectionRateLimitHandler(RateLimiter limiter,
                                      IEventCollector eventCollector,
                                      ChannelPipelineInitializer initializer) {
        rateLimiter = limiter;
        this.eventCollector = eventCollector;
        this.initializer = initializer;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        if (rateLimiter.tryAcquire()) {
            accepted = true;
            initializer.initialize(ctx.pipeline());
            ctx.fireChannelActive();
            // Remove this handler after the connection is accepted
            ctx.pipeline().remove(this);
        } else {
            accepted = false;
            log.debug("Connection dropped due to exceed limit");
            eventCollector.report(getLocal(ChannelError.class)
                .peerAddress(ChannelAttrs.socketAddress(ctx.channel()))
                .cause(new RuntimeException("Reject connection due to conn rate limiting")));
            // close the connection randomly
            ctx.channel().config().setAutoRead(false);
            ctx.executor().schedule(() -> {
                if (ctx.channel().isActive()) {
                    ctx.close();
                }
            }, ThreadLocalRandom.current().nextLong(100, 3000), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!accepted) {
            ReferenceCountUtil.release(msg);
            return;
        }
        ctx.fireChannelRead(msg);
    }

    /**
     * Initialize the pipeline when the connection is accepted.
     */
    public interface ChannelPipelineInitializer {
        void initialize(ChannelPipeline pipeline);
    }
}
