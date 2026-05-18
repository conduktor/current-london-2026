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
}
