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
import com.fasterxml.jackson.databind.DeserializationFeature;
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
 * of these, but the defaults move across point releases in either direction (e.g. {@code maxStringLength}
 * shipped at 5 MiB in 2.15.0 and was then raised to 20 MiB in 2.15.1 in response to user feedback, per
 * jackson-core issues #863 and #1014), and the defaults are not documented as part of Jackson's public
 * API surface. Pinning the values here makes the broker's behaviour stable against silent
 * dependency-bump regressions in either direction and makes the security guarantee something a reader
 * can verify from one file rather than the current Jackson release notes.
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
 *   <li>{@code maxDocumentLength = 1 MiB} — defence-in-depth duplicate of the
 *       {@code http.bridge.max.request.body.bytes} HTTP cap and the per-frame WebSocket cap. The
 *       byte caps run upstream of Jackson; this cap closes the gap on any future parse path that
 *       skips them (e.g. parsing a String built from concatenated frames, or a record value bytes
 *       buffer read after the body cap).</li>
 *   <li>{@code maxTokenCount = 200_000} — bounds the parser's work per byte. A 1 MiB body of
 *       {@code {"a":1,"a":1,...}} (~175K entries, each tokenising to {@code FIELD_NAME +
 *       VALUE_NUMBER_INT}) drives ~350K tokens through the parser and allocates one binding
 *       object per token even though the final document is a single key. The string and document
 *       caps don't catch this — both are within bounds — but the transient heap multiplier is
 *       material on a 1 MiB body (~15 MiB transient), and grows linearly with concurrent
 *       requests. 200K tokens is well above any realistic produce shape (a 1 MiB body of
 *       legitimate records reaches ~50K tokens) and well below the structural ceiling for a 1 MiB
 *       body of pathological JSON.</li>
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

    /** 1 MiB — defence-in-depth duplicate of the HTTP body cap and the per-frame WebSocket cap. */
    public static final long MAX_DOCUMENT_LENGTH = 1L << 20;

    /** Bounds per-byte parser amplification; 200K is well above realistic shapes (~50K for a full produce). */
    public static final long MAX_TOKEN_COUNT = 200_000L;

    private static final StreamReadConstraints CONSTRAINTS = StreamReadConstraints.builder()
        .maxStringLength(MAX_STRING_LENGTH)
        .maxNestingDepth(MAX_NESTING_DEPTH)
        .maxNumberLength(MAX_NUMBER_LENGTH)
        .maxDocumentLength(MAX_DOCUMENT_LENGTH)
        .maxTokenCount(MAX_TOKEN_COUNT)
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
        // Fail when a JSON object contains duplicate keys at the same nesting level (e.g.
        // {"type":"STRING","type":"BINARY"}). Jackson's default is to silently keep the LAST value, which
        // is a documented parser-confusion vector: an intermediate (firewall, audit log, content-aware
        // proxy) that records the FIRST value disagrees with the bridge about what was actually accepted,
        // and a multi-tenant attacker can exploit that disagreement to hide a payload. The bridge parses
        // attacker-controlled envelopes (value type/data, ProduceRequest body, WebSocket text frames) into
        // trees via readTree, which is exactly the path this feature guards. Honest clients never send
        // duplicate keys; rejecting the request with a 400 is the correct safe-by-default behaviour.
        mapper.enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
        // Fail when the input stream contains tokens AFTER a valid root value. Default in Jackson 2.x is to silently
        // accept and discard trailing tokens, so {@code mapper.readTree("{\"a\":1}garbage")} succeeds and returns the
        // {@code {"a":1}} tree. The bridge passes the same hardened mapper to three attacker-controlled readTree call
        // sites — HTTP body (KafkaHttpServlet), WS subscribe text frame (WsSubscribeMessageParser), and record-value
        // envelope (ValueSerializer) — none of which document or test for trailing content. A request smuggler that
        // wedges valid JSON followed by an arbitrary tail can: (a) pass content-aware proxies that parse only the
        // valid prefix; (b) confuse audit / SIEM pipelines that record the raw bytes vs the bridge's accepted view;
        // (c) sneak a second JSON object into one body that gets billed as a single request. Jackson 3.0 flips this
        // default to {@code true} per the project's own security guidance — pin it here so the broker's behaviour
        // does not depend on the Jackson major-version bump landing.
        mapper.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        return mapper;
    }
}
