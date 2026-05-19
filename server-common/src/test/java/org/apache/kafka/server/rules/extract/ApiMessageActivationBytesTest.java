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
package org.apache.kafka.server.rules.extract;

import org.apache.kafka.common.message.EnvelopeRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.server.rules.cel.CelCompiler;
import org.apache.kafka.server.rules.cel.CelProgram;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the {@code byte[]} / {@code ByteBuffer} → {sizeInBytes} normalisation in
 * the activation walker. Sits in its own class — not in
 * {@link ApiMessageActivationTest} — so the dependency on a fresh set of
 * generated DTO types does not push the parent over checkstyle's Class Data
 * Abstraction Coupling cap. Same rationale as
 * {@link ApiMessageActivationUuidTest}.
 *
 * <p>Contract under test (audit round-6 findings B1 BLOCKER + H1 HIGH): every
 * non-credential {@code byte[]} and {@code ByteBuffer} field on a Kafka
 * protocol message surfaces as a tiny {@code {sizeInBytes: long}} descriptor —
 * NOT as a live byte buffer that CEL has no usable contract for and that pins
 * arbitrarily-large request payloads in the activation map. Credential-bearing
 * {@code byte[]} fields (authBytes, salt, saltedPassword, hmac) are stripped
 * earlier by {@code SENSITIVE_NAMES} and are covered by
 * {@code ApiMessageActivationTest}; the descriptor branch here is the
 * defence-in-depth for everything else (envelope payload, telemetry blob,
 * consumer subscription metadata, raft voter directory id, etc.).
 *
 * <p>The hazard this contract closes: with raw passthrough, a rule of the form
 * {@code request.requestData == b"..."} compares {@code ByteBuffer} by JVM
 * identity (silently false even for content-equal buffers) or raises a CEL
 * type mismatch — the engine then catches at evaluate-time and fails OPEN.
 * Operators writing the natural byte-prefix DENY rule observe a rule that
 * appears installed but never fires. Equally important, the envelope payload
 * on inter-broker forwarding can be many MiB; pinning it in the activation
 * map for every rule evaluation is a memory hazard and a defence-in-depth gap
 * against credential exfiltration through the audit channel.
 */
public class ApiMessageActivationBytesTest {

    // R45-A-1: symmetric defence for CelLimits.STEPS ThreadLocal. Test pool
    // threads are reused across test classes (build.gradle:504 forks reuse
    // workers); a STEPS counter left near the 100k limit by a prior class
    // would soft-brick the first evalBoolean here with budget-exceeded.
    // Mirrors CelProgramTest @BeforeEach/@AfterEach — see that class's
    // javadoc which names ApiMessageActivation*Test as the at-risk neighbour.
    @BeforeEach
    public void resetStepBudget() {
        CelProgram.resetEvalStepBudget();
    }

    @AfterEach
    public void leaveCounterClean() {
        CelProgram.resetEvalStepBudget();
    }

    @Test
    public void byteBufferFieldSurfacesAsSizeInBytesDescriptor() {
        byte[] payload = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        EnvelopeRequestData env = new EnvelopeRequestData()
            .setRequestData(ByteBuffer.wrap(payload));

        Map<String, Object> m = ApiMessageActivation.from(env);
        Object requestData = m.get("requestData");
        assertTrue(requestData instanceof Map,
            "ByteBuffer must surface as a {sizeInBytes} map, was "
                + (requestData == null ? "null" : requestData.getClass()));
        Map<?, ?> descriptor = (Map<?, ?>) requestData;
        assertEquals(1, descriptor.size(), "descriptor must carry only sizeInBytes");
        assertEquals((long) payload.length, descriptor.get("sizeInBytes"));
        // Belt-and-braces: the live buffer must NOT have leaked through under
        // any other key. A rule probing `requestData.position` or
        // `requestData.array` must observe absence, not a walkable view of
        // the inter-broker envelope payload (which can carry serialised
        // inner SASL-bearing requests).
        assertFalse(descriptor.containsKey("position"));
        assertFalse(descriptor.containsKey("array"));
        assertFalse(descriptor.containsKey("remaining"));
    }

    @Test
    public void emptyByteBufferSurfacesAsZeroSizeDescriptor() {
        EnvelopeRequestData env = new EnvelopeRequestData()
            .setRequestData(ByteBuffer.allocate(0));
        Map<String, Object> m = ApiMessageActivation.from(env);
        Map<?, ?> descriptor = (Map<?, ?>) m.get("requestData");
        assertEquals(0L, descriptor.get("sizeInBytes"));
    }

    @Test
    public void byteArrayFieldSurfacesAsSizeInBytesDescriptor() {
        // JoinGroupRequestProtocol.metadata is a non-credential byte[] field —
        // it carries the consumer's subscription metadata, not a secret, so
        // SENSITIVE_NAMES does not strip it at the toMap level. This is the
        // shape that audit finding H1 reaches: a rule author writing
        // `request.protocols.exists(p, p.metadata == b"...")` previously got
        // raw byte[] back through `convertScalar` and a silently-failing
        // comparison (Java byte[] equality is identity, not content). With the
        // {sizeInBytes} descriptor the rule shape is at least well-defined:
        // `request.protocols.exists(p, p.metadata.sizeInBytes > N)`.
        byte[] metadata = new byte[]{42, 42, 42};
        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols =
            new JoinGroupRequestData.JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("range").setMetadata(metadata));
        JoinGroupRequestData jg = new JoinGroupRequestData()
            .setGroupId("g")
            .setProtocols(protocols);

        Map<String, Object> m = ApiMessageActivation.from(jg);
        List<?> ps = (List<?>) m.get("protocols");
        Map<?, ?> first = (Map<?, ?>) ps.get(0);
        Object md = first.get("metadata");
        assertTrue(md instanceof Map,
            "byte[] must surface as a {sizeInBytes} map, was "
                + (md == null ? "null" : md.getClass()));
        Map<?, ?> descriptor = (Map<?, ?>) md;
        assertEquals(1, descriptor.size());
        assertEquals((long) metadata.length, descriptor.get("sizeInBytes"));
    }

    @Test
    public void sizeInBytesIsAddressableFromCelRules() {
        // End-to-end: the size-bounded DENY rule shape that operators reach
        // for must actually evaluate true / false. If this regresses,
        // size-bounded byte-field DENY rules silently never fire.
        byte[] payload = new byte[1024];
        EnvelopeRequestData env = new EnvelopeRequestData()
            .setRequestData(ByteBuffer.wrap(payload));

        Map<String, Object> activation = ApiMessageActivation.requestActivation(env);
        CelProgram hits = CelCompiler.compile("request.requestData.sizeInBytes > 512");
        assertTrue(hits.evalBoolean(activation::get));

        CelProgram misses = CelCompiler.compile("request.requestData.sizeInBytes > 2048");
        assertFalse(misses.evalBoolean(activation::get));
    }
}
