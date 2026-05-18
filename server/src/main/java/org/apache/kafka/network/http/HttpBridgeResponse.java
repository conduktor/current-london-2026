/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.network.http;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Objects;

/**
 * The bridge's transport-agnostic response — what the Jetty handler (or any other adapter) needs to write back to the
 * client. Status code, JSON body, optional {@code Retry-After} seconds.
 *
 * <p>This is the convergence type the two formatters' own {@code Formatted} classes are projected into. Keeping it
 * separate from the formatter-internal types lets each formatter own its assembly logic without coupling its callers
 * to its inner classes.
 */
public final class HttpBridgeResponse {

    private final int status;
    private final JsonNode body;
    private final int retryAfterSeconds;

    public HttpBridgeResponse(int status, JsonNode body, int retryAfterSeconds) {
        this.status = status;
        this.body = Objects.requireNonNull(body, "body must not be null");
        this.retryAfterSeconds = retryAfterSeconds;
    }

    public int status() {
        return status;
    }

    public JsonNode body() {
        return body;
    }

    public boolean hasRetryAfter() {
        return retryAfterSeconds > 0;
    }

    public int retryAfterSeconds() {
        return retryAfterSeconds;
    }
}
