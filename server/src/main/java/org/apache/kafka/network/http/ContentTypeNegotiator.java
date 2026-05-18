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
 * <p>The fetch response body carries HAL-style {@code _links}, so a client may legitimately ask for
 * {@code application/hal+json} (HAL is JSON, but the more specific type signals the body shape contract). When the
 * client explicitly accepts {@code application/hal+json}, we honour it; otherwise we default to
 * {@code application/json} — including for {@code &#42;/&#42;} and absent headers, so existing clients see no change.
 *
 * <p>Parsing is deliberately minimal: split on comma, strip parameters after {@code ;}, match the bare media type.
 * Q-values are ignored — a client that lists {@code application/hal+json;q=0.1, application/json;q=0.9} is still given
 * HAL+JSON because the body remains valid JSON either way. We are not building a generic RFC 7231 conneg engine; we are
 * answering one yes/no question about one specific media type.
 */
final class ContentTypeNegotiator {

    static final String APPLICATION_JSON = "application/json";
    static final String APPLICATION_HAL_JSON = "application/hal+json";

    private ContentTypeNegotiator() { }

    /** Returns {@link #APPLICATION_HAL_JSON} if the client explicitly listed it, otherwise {@link #APPLICATION_JSON}. */
    static String resolve(String acceptHeader) {
        if (acceptHeader == null || acceptHeader.isEmpty()) {
            return APPLICATION_JSON;
        }
        for (String range : acceptHeader.split(",")) {
            String mediaType = range.trim();
            int semicolon = mediaType.indexOf(';');
            if (semicolon >= 0) {
                mediaType = mediaType.substring(0, semicolon).trim();
            }
            if (APPLICATION_HAL_JSON.equalsIgnoreCase(mediaType)) {
                return APPLICATION_HAL_JSON;
            }
        }
        return APPLICATION_JSON;
    }
}
