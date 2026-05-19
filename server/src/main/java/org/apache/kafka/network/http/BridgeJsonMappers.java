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

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Factory for {@link ObjectMapper} instances used by the HTTP bridge to parse untrusted JSON
 * (request bodies, WebSocket text frames, record values that claim {@code application/json}).
 *
 * <p>Every parse path that touches attacker-controlled bytes must use one of these mappers so the
 * Jackson {@link StreamReadConstraints} defence-in-depth caps apply. The bridge already enforces a
 * total body-byte cap upstream ({@code BoundedRequestBody}, the per-frame WebSocket limit, and the
 * {@code BoundedInputStream} on record reads), but those caps bound only the raw byte count of one
 * message — they do not bound the parser's work per byte. A pathological JSON document with a 1 KiB
 * deeply-nested object or a single 256 MiB string within an otherwise small envelope would still
 * succeed at the byte cap and then blow the parser stack or heap. The Jackson 2.15+ constraints
 * close that amplification by failing the parse early with a {@link com.fasterxml.jackson.core.exc.StreamConstraintsException}.
 *
 * <p><strong>Why the explicit limits.</strong> Jackson's package-level defaults already cap several
 * of these, but the defaults are quietly tightened across point releases (the {@code maxStringLength}
 * default dropped from 20 MiB → 5 MiB in 2.15 and may move again), and the defaults are not
 * documented as part of Jackson's public API surface. Pinning the values here makes the broker's
 * behaviour stable against silent dependency-bump regressions and makes the security guarantee
 * something a reader can verify from one file rather than the current Jackson release notes.
 *
 * <p><strong>The chosen values.</strong>
 * <ul>
 *   <li>{@code maxStringLength = 1 MiB} — covers every realistic string in this protocol (topic
 *       names ≤ 249 bytes, cursors ≤ 512 bytes, base64-encoded record values bounded by
 *       {@code http.bridge.max.request.body.bytes}). One megabyte leaves comfortable headroom for
 *       a large single record value coming through an {@code application/json} content type; far
 *       above that points to an attacker, not a legitimate client.</li>
 *   <li>{@code maxNestingDepth = 64} — the bridge JSON shapes are at most about 6 levels deep
 *       ({@code records[]} → record-object → value-envelope → data → arbitrary user JSON). A user
 *       posting JSON via the {@code application/json} value type can nest, but a depth of 64 is
 *       so far beyond legitimate use that the cap is invisible to honest clients while turning the
 *       classic billion-laughs / stack-blow amplifier into a clean 400.</li>
 *   <li>{@code maxNumberLength = 1_000} — defends against gigabyte-long numeric literals
 *       (BigDecimal parsing is the textbook Jackson DoS shape). The bridge never reads numbers
 *       longer than {@code Long.MAX_VALUE}'s 19 digits.</li>
 * </ul>
 *
 * <p>None of these caps are surfaced via configuration. They are security floors, not tuneables.
 * A future release that needs to raise them must do so as a deliberate code change reviewed in the
 * context of all four parse paths simultaneously, not as an operator setting that one tenant can
 * relax for their cluster.
 */
public final class BridgeJsonMappers {

    /** 1 MiB — comfortably above the largest legitimate string in any bridge JSON shape. */
    public static final int MAX_STRING_LENGTH = 1 << 20;

    /** Bridge JSON shapes are ≤ 6 deep natively; 64 absorbs any reasonable nested JSON record value. */
    public static final int MAX_NESTING_DEPTH = 64;

    /** Numeric literals in this protocol top out at 19 digits ({@code Long.MAX_VALUE}). */
    public static final int MAX_NUMBER_LENGTH = 1_000;

    private static final StreamReadConstraints CONSTRAINTS = StreamReadConstraints.builder()
        .maxStringLength(MAX_STRING_LENGTH)
        .maxNestingDepth(MAX_NESTING_DEPTH)
        .maxNumberLength(MAX_NUMBER_LENGTH)
        .build();

    private BridgeJsonMappers() {
    }

    /**
     * Returns a new {@link ObjectMapper} pre-configured with the bridge's hardening constraints.
     * Callers receive a fresh instance — {@code ObjectMapper} is thread-safe for read/write but
     * not for configuration changes, and handing out a shared singleton would invite later code
     * to reconfigure the global state of every other bridge consumer.
     */
    public static ObjectMapper hardened() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.getFactory().setStreamReadConstraints(CONSTRAINTS);
        return mapper;
    }
}
