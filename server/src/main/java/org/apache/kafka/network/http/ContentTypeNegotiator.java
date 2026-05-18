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

/**
 * Picks the {@code Content-Type} for a successful HTTP bridge response from the client's {@code Accept} header.
 *
 * <p>Three outputs:
 * <ul>
 *   <li>{@link #TEXT_EVENT_STREAM} when the client wants Server-Sent Events — a different wire protocol on the same
 *       URL. This wins outright: a client asking for {@code text/event-stream} is asking for a streaming response,
 *       not a one-shot JSON page, so HAL+JSON / JSON preferences don't apply.</li>
 *   <li>{@link #APPLICATION_HAL_JSON} when the client explicitly lists HAL+JSON anywhere in the Accept list. HAL is
 *       still JSON, but the more specific media type signals the body-shape contract clients build against.</li>
 *   <li>{@link #APPLICATION_JSON} for everything else, including absent or wildcard Accept — keeps existing clients
 *       on the byte-identical response they already see.</li>
 * </ul>
 *
 * <p>Parsing is deliberately minimal: split on comma, strip parameters after {@code ;}, match the bare media type.
 * Q-values are ignored — a client that lists {@code application/hal+json;q=0.1, application/json;q=0.9} is still given
 * HAL+JSON because the body remains valid JSON either way. We are not building a generic RFC 7231 conneg engine; we are
 * answering yes/no questions about specific media types.
 */
final class ContentTypeNegotiator {

    static final String APPLICATION_JSON = "application/json";
    static final String APPLICATION_HAL_JSON = "application/hal+json";
    static final String TEXT_EVENT_STREAM = "text/event-stream";

    private ContentTypeNegotiator() { }

    /**
     * Returns {@link #TEXT_EVENT_STREAM} if the client asked for SSE, otherwise {@link #APPLICATION_HAL_JSON} if HAL was
     * listed, otherwise {@link #APPLICATION_JSON}.
     */
    static String resolve(String acceptHeader) {
        if (acceptHeader == null || acceptHeader.isEmpty()) {
            return APPLICATION_JSON;
        }
        boolean sawHal = false;
        for (String range : acceptHeader.split(",")) {
            String mediaType = range.trim();
            int semicolon = mediaType.indexOf(';');
            if (semicolon >= 0) {
                mediaType = mediaType.substring(0, semicolon).trim();
            }
            if (TEXT_EVENT_STREAM.equalsIgnoreCase(mediaType)) {
                return TEXT_EVENT_STREAM;
            }
            if (APPLICATION_HAL_JSON.equalsIgnoreCase(mediaType)) {
                sawHal = true;
            }
        }
        return sawHal ? APPLICATION_HAL_JSON : APPLICATION_JSON;
    }
}
