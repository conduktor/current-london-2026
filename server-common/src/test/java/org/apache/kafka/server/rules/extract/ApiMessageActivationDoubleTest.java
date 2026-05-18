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

import org.apache.kafka.common.message.AlterClientQuotasRequestData;
import org.apache.kafka.common.message.AlterClientQuotasRequestData.EntryData;
import org.apache.kafka.common.message.AlterClientQuotasRequestData.OpData;
import org.apache.kafka.server.rules.cel.CelCompiler;
import org.apache.kafka.server.rules.cel.CelProgram;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the Double/Float → null (opaque) normalisation in the activation
 * walker. Sits in its own class — not in {@link ApiMessageActivationTest} —
 * so the dependency on {@link AlterClientQuotasRequestData} does not push
 * the parent class over checkstyle's Class Data Abstraction Coupling cap.
 * Same rationale as {@link ApiMessageActivationUuidTest} and
 * {@link ApiMessageActivationBytesTest}.
 *
 * <p>Contract under test (audit round-7 finding H2 HIGH): any
 * {@code double} or {@code float} field on a Kafka protocol message
 * surfaces as {@code null}, NOT as a {@link Double}/{@link Float}
 * passthrough. The walker historically returned the boxed wrapper
 * unchanged via the generic {@code if (v instanceof Number) return v;}
 * branch, which created a silent-truncation hazard at evaluation time:
 *
 * <pre>{@code
 *   // CelNode.valueEquals / compareValues coerce both sides via
 *   // ((Number) v).longValue(), so a rule like
 *   //   request.ops.exists(op, op.value == 1)
 *   // would match OpData.value = 1.0, 1.49, 0.5, 0.9 silently — the
 *   // operator authoring a quota-bound DENY rule has no way to know the
 *   // predicate fires on the wrong subset.
 * }</pre>
 *
 * <p>The honest fix is to surface Double/Float as the activation key's
 * value, but {@code null} — comparisons then evaluate to false
 * consistently (fail-noisy-on-shape) rather than fail-silent-on-
 * truncation. Operators who genuinely need numeric float access in
 * rules see the predicate never match and surface that requirement
 * explicitly, rather than discovering the truncation in production.
 * The only Kafka protocol message currently affected is
 * {@code AlterClientQuotasRequestData.OpData.value} (the quota value);
 * the same contract closes any future {@code float64}/{@code float32}
 * schema field.
 */
public class ApiMessageActivationDoubleTest {

    private static AlterClientQuotasRequestData reqWithOpValue(double v) {
        OpData op = new OpData().setKey("producer_byte_rate").setValue(v).setRemove(false);
        EntryData entry = new EntryData().setOps(Collections.singletonList(op));
        return new AlterClientQuotasRequestData().setEntries(Collections.singletonList(entry));
    }

    @Test
    public void doubleFieldSurfacesAsNull() {
        // OpData.value is the canonical double field on the Kafka
        // protocol surface. Walking it must produce a `value: null`
        // entry in the activation map, NOT a `value: 1.5` Double
        // passthrough that the CEL comparator would silently truncate.
        AlterClientQuotasRequestData req = reqWithOpValue(1.5);

        Map<String, Object> m = ApiMessageActivation.from(req);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> es = (List<Map<String, Object>>) m.get("entries");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> os = (List<Map<String, Object>>) es.get(0).get("ops");
        Map<String, Object> op = os.get(0);
        // Belt-and-braces: the key MUST be present (otherwise rules
        // probing op.value would resolve through Field.eval to null
        // via "not in map" rather than "explicitly null", which has
        // the same observable behaviour today but is a less precise
        // contract — keep the key, redact the value).
        assertTrue(op.containsKey("value"), "value key must be present");
        assertNull(op.get("value"),
            "Double field must surface as null, was "
                + (op.get("value") == null ? "null" : op.get("value").getClass()));
    }

    @Test
    public void zeroAndIntegralDoublesAlsoSurfaceAsNull() {
        // Defence-in-depth: the normalisation must apply uniformly,
        // not just to fractional values. An operator who writes
        //   request.entries.exists(e, e.ops.exists(op, op.value == 0))
        // expecting to match the integral-zero quota would otherwise
        // hit a silent always-false on Double(0.0) — but ONLY for the
        // values they typed as integers in the predicate; fractional
        // values would silently match. The contract is uniform: ALL
        // doubles surface as null.
        AlterClientQuotasRequestData req = reqWithOpValue(0.0);

        Map<String, Object> m = ApiMessageActivation.from(req);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> es = (List<Map<String, Object>>) m.get("entries");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> os = (List<Map<String, Object>>) es.get(0).get("ops");
        assertNull(os.get(0).get("value"));
    }

    @Test
    public void doubleFieldComparisonInRuleAlwaysFalse() {
        // End-to-end: the would-be-silent rule shape must consistently
        // evaluate false rather than match on the wrong subset. An
        // operator writing `op.value == 1` had a 50/50 chance of
        // accidental match against any Double in [0.5, 1.49]. With
        // the null surface, the predicate is now reliably false —
        // forcing the operator to either drop the predicate or ask
        // for a documented numeric-float shape.
        AlterClientQuotasRequestData req = reqWithOpValue(1.0);

        Map<String, Object> activation = ApiMessageActivation.requestActivation(req);
        // The predicate operators reach for after seeing OpData.value
        // in the docs. Pre-fix this returned true for value=1.0
        // because longValue() truncated. Post-fix this returns false
        // because the surface value is null.
        CelProgram p = CelCompiler.compile(
            "request.entries.exists(e, e.ops.exists(op, op.value == 1))");
        assertFalse(p.evalBoolean(activation::get),
            "double-field predicate must NOT match — surface is null");
    }
}
