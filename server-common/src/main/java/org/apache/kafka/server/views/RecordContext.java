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
package org.apache.kafka.server.views;

import java.util.List;
import java.util.Optional;

/**
 * The bindings exposed to a predicate when it evaluates against a single record.
 *
 * Implementations are expected to be cheap to construct (lazy parsing of {@code body},
 * lazy UTF-8 decoding of headers) so that filtering a batch of N records is O(N) in the
 * number of fields actually touched by the predicate, not N × (total fields in the record).
 *
 * Any failure to extract a value (malformed JSON, invalid UTF-8, byte caps exceeded,
 * unknown header) returns {@link Optional#empty()} — the evaluator interprets empty as
 * "missing/unknown" and propagates skip-record semantics.
 */
public interface RecordContext {

    /** Raw key bytes, or null if the record has no key. */
    byte[] rawKey();

    /** Raw value bytes, or null if the record has no value. */
    byte[] rawBody();

    /**
     * Raw header bytes for {@code name}, or {@code null} if the header is not set on the record.
     * Used alongside {@link #header(String)} to distinguish "header absent" (null value, predicate
     * sees {@code null}) from "header present but bytes are not valid UTF-8" (SKIP record). The
     * spec (PROMPT.md scenario list) requires invalid-UTF-8 headers to silently skip the record
     * rather than falsely satisfy {@code header != literal} via a null bypass.
     */
    byte[] rawHeader(String name);

    /** UTF-8 decode of the raw key, or empty if the key is null or invalid UTF-8. */
    Optional<String> keyAsString();

    /**
     * UTF-8 decode of a header value by name. Empty if the header doesn't exist or the bytes
     * are not valid UTF-8. Header lookups do not throw — invalid headers are silently treated
     * as missing, as required by the spec's "malformed headers must not crash the fetch" rule.
     * Callers that need to distinguish absent from undecodable must pair this with
     * {@link #rawHeader(String)}.
     */
    Optional<String> header(String name);

    /**
     * Sentinel returned by {@link #bodyAt(java.util.List)} when the body is unusable (parse
     * error, exceeds {@code maxBodyBytes}, exceeds {@code maxJsonDepth}, malformed UTF-8 inside
     * the JSON). Compare with reference equality, never with {@code .equals(...)}.
     */
    Object BODY_UNUSABLE = new Object();

    /**
     * Parsed JSON body, navigated by the supplied accessor path (each accessor is a field name
     * or quoted-string key). Three-state return — compare {@link #BODY_UNUSABLE} by identity:
     *  - {@link #BODY_UNUSABLE} : record is malformed; evaluator turns this into a record-skip.
     *  - {@code null}           : path doesn't resolve (missing field, JSON null, indexing
     *                             into a scalar). Treated as the value {@code null}
     *                             ("unknown"), which makes comparisons unequal to anything
     *                             non-null and short-circuits logical operators to falsy.
     *  - scalar                 : leaf value, one of {@code Long}, {@code Double},
     *                             {@code String}, or {@code Boolean}.
     */
    Object bodyAt(List<String> path);

    long offset();
    int partition();
    long timestamp();
}
