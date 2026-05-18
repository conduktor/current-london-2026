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
 * Q-value <em>preference</em> ordering is ignored — a client that lists
 * {@code application/hal+json;q=0.1, application/json;q=0.9} is still given HAL+JSON because the body remains valid
 * JSON either way. We are not building a generic RFC 7231 conneg engine; we are answering yes/no questions about
 * specific media types.
 *
 * <p>The one q-value we <em>do</em> honour is {@code q=0}: per RFC 9110 §12.5.1 that means "not acceptable", which is
 * a yes/no answer rather than a preference. A client sending {@code Accept: text/event-stream;q=0, application/json}
 * is explicitly refusing SSE; handing it an event stream would break a strict JSON parser. Same for HAL.
 */
final class ContentTypeNegotiator {

    static final String APPLICATION_JSON = "application/json";
    static final String APPLICATION_HAL_JSON = "application/hal+json";
    static final String TEXT_EVENT_STREAM = "text/event-stream";

    private ContentTypeNegotiator() { }

    /**
     * Returns {@link #TEXT_EVENT_STREAM} if the client asked for SSE, otherwise {@link #APPLICATION_HAL_JSON} if HAL was
     * listed, otherwise {@link #APPLICATION_JSON}. Media types with {@code q=0} are skipped.
     */
    static String resolve(String acceptHeader) {
        if (acceptHeader == null || acceptHeader.isEmpty()) {
            return APPLICATION_JSON;
        }
        boolean sawHal = false;
        for (String range : acceptHeader.split(",")) {
            String entry = range.trim();
            int semicolon = entry.indexOf(';');
            String mediaType;
            String params;
            if (semicolon >= 0) {
                mediaType = entry.substring(0, semicolon).trim();
                params = entry.substring(semicolon + 1);
            } else {
                mediaType = entry;
                params = "";
            }
            if (isExplicitlyRejected(params)) {
                continue;
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

    /**
     * RFC 9110 §12.5.1: {@code q=0} ("0", "0.", "0.0", "0.00", "0.000") means the media type is "not acceptable".
     * All other q-values — including malformed ones — are treated as no rejection (default acceptability). This is
     * the only q-value the negotiator inspects; preference ordering above zero stays ignored on purpose.
     */
    private static boolean isExplicitlyRejected(String params) {
        if (params.isEmpty()) {
            return false;
        }
        for (String param : params.split(";")) {
            String p = param.trim();
            int eq = p.indexOf('=');
            if (eq < 0) {
                continue;
            }
            String name = p.substring(0, eq).trim();
            if (!"q".equalsIgnoreCase(name)) {
                continue;
            }
            String value = p.substring(eq + 1).trim();
            try {
                if (Double.parseDouble(value) == 0.0) {
                    return true;
                }
            } catch (NumberFormatException ignored) {
                // Malformed q-value: leave the media type acceptable rather than guessing intent.
            }
        }
        return false;
    }
}
