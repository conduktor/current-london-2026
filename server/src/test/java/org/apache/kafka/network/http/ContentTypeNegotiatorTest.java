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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ContentTypeNegotiatorTest {

    @Test
    void nullOrEmptyAcceptHeaderResolvesToJson() {
        assertEquals("application/json", ContentTypeNegotiator.resolve(null));
        assertEquals("application/json", ContentTypeNegotiator.resolve(""));
    }

    @Test
    void explicitHalJsonResolvesToHalJson() {
        assertEquals("application/hal+json", ContentTypeNegotiator.resolve("application/hal+json"));
    }

    @Test
    void halJsonInListResolvesToHalJson() {
        // Real clients send q-weighted lists; the negotiator picks HAL whenever it's listed at all because the body
        // is valid JSON in both shapes.
        assertEquals("application/hal+json",
            ContentTypeNegotiator.resolve("application/json, application/hal+json"));
        assertEquals("application/hal+json",
            ContentTypeNegotiator.resolve("application/hal+json;q=1.0, application/json;q=0.9"));
    }

    @Test
    void halJsonWithParametersStillMatches() {
        // ";q=" or ";charset=" suffixes on the requested media range must not break matching.
        assertEquals("application/hal+json",
            ContentTypeNegotiator.resolve("application/hal+json; charset=utf-8"));
        assertEquals("application/hal+json",
            ContentTypeNegotiator.resolve("application/hal+json;q=0.5"));
    }

    @Test
    void halJsonMatchingIsCaseInsensitive() {
        // RFC 7231: media-type tokens are case-insensitive.
        assertEquals("application/hal+json",
            ContentTypeNegotiator.resolve("Application/HAL+JSON"));
    }

    @Test
    void plainJsonOrWildcardResolvesToJson() {
        // Existing clients sending Accept: application/json or */* must see exactly the prior content-type.
        assertEquals("application/json", ContentTypeNegotiator.resolve("application/json"));
        assertEquals("application/json", ContentTypeNegotiator.resolve("*/*"));
        assertEquals("application/json", ContentTypeNegotiator.resolve("application/*"));
        assertEquals("application/json", ContentTypeNegotiator.resolve("text/html"));
    }

    @Test
    void eventStreamWinsOverHalAndJson() {
        // SSE is a different wire protocol on the same URL; if the client asks for it, that's what they get even when
        // they also list JSON / HAL+JSON as fallbacks.
        assertEquals("text/event-stream", ContentTypeNegotiator.resolve("text/event-stream"));
        assertEquals("text/event-stream",
            ContentTypeNegotiator.resolve("text/event-stream, application/json"));
        assertEquals("text/event-stream",
            ContentTypeNegotiator.resolve("application/hal+json, text/event-stream"));
        assertEquals("text/event-stream",
            ContentTypeNegotiator.resolve("text/event-stream;q=0.9"));
        assertEquals("text/event-stream",
            ContentTypeNegotiator.resolve("Text/Event-Stream"));
    }

    @Test
    void zeroQValueRejectsMediaType() {
        // RFC 9110 §12.5.1: q=0 means "not acceptable". A client that lists text/event-stream;q=0 is explicitly
        // refusing SSE — handing it a stream regardless would break strict JSON consumers. All canonical q=0
        // spellings ("0", "0.0", "0.00", "0.000") must reject; whitespace around the '=' must not defeat the check.
        assertEquals("application/json",
            ContentTypeNegotiator.resolve("text/event-stream;q=0, application/json"));
        assertEquals("application/json",
            ContentTypeNegotiator.resolve("text/event-stream;q=0.0, application/json"));
        assertEquals("application/json",
            ContentTypeNegotiator.resolve("text/event-stream;q=0.000, application/json"));
        assertEquals("application/json",
            ContentTypeNegotiator.resolve("text/event-stream ; q = 0 , application/json"));
        // Capital Q must also be honoured — parameter names are case-insensitive in RFC 9110.
        assertEquals("application/json",
            ContentTypeNegotiator.resolve("text/event-stream;Q=0, application/json"));
        // HAL with q=0 must fall through to plain JSON, not be elevated.
        assertEquals("application/json",
            ContentTypeNegotiator.resolve("application/hal+json;q=0, application/json"));
        // A single rejected media type with no fallback still resolves to JSON (the documented default for
        // "everything else, including absent or wildcard Accept").
        assertEquals("application/json",
            ContentTypeNegotiator.resolve("text/event-stream;q=0"));
        // Non-zero q-values do NOT reject — the negotiator only honours q=0, not preference ordering.
        assertEquals("text/event-stream",
            ContentTypeNegotiator.resolve("text/event-stream;q=0.001, application/json"));
        // Malformed q-values must not cause the media type to disappear: better to over-accept than to silently
        // reject a client that fat-fingered its Accept header.
        assertEquals("text/event-stream",
            ContentTypeNegotiator.resolve("text/event-stream;q=banana, application/json"));
    }
}
