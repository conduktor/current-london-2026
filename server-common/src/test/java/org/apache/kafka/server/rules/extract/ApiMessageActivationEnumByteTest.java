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

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.server.rules.cel.CelCompiler;
import org.apache.kafka.server.rules.cel.CelProgram;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the contract that enum-coded {@code byte} fields on Kafka protocol
 * messages surface as a numeric {@code Long}, NOT as a symbolic string. Sits
 * in its own class — not in {@link ApiMessageActivationTest} — so the
 * dependency on {@link FetchRequestData} does not push the parent class
 * over checkstyle's Class Data Abstraction Coupling cap. Same rationale as
 * the sister {@code Uuid}/{@code Bytes}/{@code Double} pinning classes.
 *
 * <p>Audit round-7 finding H4 (HIGH). The Kafka schema encodes several
 * semantic enums as {@code byte} fields:
 * <ul>
 *   <li>{@code FetchRequestData.isolationLevel} — {@code 0=READ_UNCOMMITTED},
 *       {@code 1=READ_COMMITTED}.</li>
 *   <li>{@code FindCoordinatorRequestData.keyType} —
 *       {@code 0=GROUP}, {@code 1=TRANSACTION}.</li>
 *   <li>{@code UpdateFeaturesRequest.UpgradeType}.</li>
 *   <li>{@code AlterPartitionRequest.LeaderRecoveryState}.</li>
 *   <li>{@code IncrementalAlterConfigsRequest.AlterableConfig.configOperation}
 *       — {@code 0=SET}, {@code 1=DELETE}, {@code 2=APPEND}, {@code 3=SUBTRACT}.</li>
 * </ul>
 *
 * <p>The walker normalises {@code Integer}/{@code Short}/{@code Byte} to
 * {@code Long} (so all numeric scalars share one CEL contract). An operator
 * reaching for the natural symbolic predicate
 * {@code request.isolationLevel == "READ_COMMITTED"} will therefore get
 * a silent always-false comparison (Long vs String → not equal at
 * {@code CelNode.valueEquals}). The correct shape is
 * {@code request.isolationLevel == 1}.
 *
 * <p>The deliberate decision recorded by this test: do NOT hand-code the
 * byte→name mapping per field. The walker's design constraint is "no
 * per-API hand-coded logic" — adding {@code isolationLevelName} would
 * either require an enum registry that gets out of date as the schema
 * evolves, or per-field code that the audit explicitly rejects. The
 * cheaper honest answer is to document the contract loudly and lock it
 * with this pinning test. Rule authors get the Long contract, in
 * exchange for a magic-number predicate that needs a comment naming the
 * code. This is an explicit ergonomics tradeoff, not a bug to be fixed
 * later by surprise.
 */
public class ApiMessageActivationEnumByteTest {

    @Test
    public void isolationLevelSurfacesAsLongNotString() {
        FetchRequestData fr = new FetchRequestData()
            .setReplicaId(-1)
            .setMaxWaitMs(500)
            .setMinBytes(1)
            .setIsolationLevel((byte) 1); // READ_COMMITTED

        Map<String, Object> m = ApiMessageActivation.from(fr);
        Object il = m.get("isolationLevel");
        assertTrue(il instanceof Long,
            "isolationLevel must surface as Long, was "
                + (il == null ? "null" : il.getClass()));
        assertEquals(1L, il);
    }

    @Test
    public void readUncommittedIsolationSurfacesAsZeroLong() {
        // Defence-in-depth: zero-coded enum values must still surface
        // as Long(0), not as a falsy "" or null. Operators writing
        //   request.isolationLevel == 0
        // need a stable type contract on both sides.
        FetchRequestData fr = new FetchRequestData()
            .setReplicaId(-1)
            .setIsolationLevel((byte) 0); // READ_UNCOMMITTED

        Map<String, Object> m = ApiMessageActivation.from(fr);
        assertEquals(0L, m.get("isolationLevel"));
    }

    @Test
    public void symbolicPredicateSilentlyFails() {
        // End-to-end pin: a rule written in the symbolic shape that
        // would seem natural to an operator coming from the Kafka
        // protocol docs evaluates to false even when the underlying
        // request IS read-committed. This locks the contract: anyone
        // who tries to convert the walker to surface names later
        // must update this test, forcing the change to surface as a
        // visible breaking-test event rather than a silent ergonomic
        // shift.
        FetchRequestData fr = new FetchRequestData()
            .setReplicaId(-1)
            .setIsolationLevel((byte) 1); // READ_COMMITTED

        Map<String, Object> activation = ApiMessageActivation.requestActivation(fr);
        CelProgram symbolic = CelCompiler.compile(
            "request.isolationLevel == \"READ_COMMITTED\"");
        assertFalse(symbolic.evalBoolean(activation::get),
            "symbolic enum predicate must NOT match — engine contract is numeric");

        // The supported shape: numeric comparison with the int8 code.
        CelProgram numeric = CelCompiler.compile(
            "request.isolationLevel == 1");
        assertTrue(numeric.evalBoolean(activation::get),
            "numeric enum predicate must match — Long(1) == 1");
    }
}
