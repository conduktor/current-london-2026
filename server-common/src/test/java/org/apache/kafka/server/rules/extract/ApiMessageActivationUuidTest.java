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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.server.rules.cel.CelCompiler;
import org.apache.kafka.server.rules.cel.CelProgram;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the Uuid → String normalisation in the activation walker. Sits in its
 * own class — not in {@link ApiMessageActivationTest} — so the dependency on
 * {@link Uuid} does not push the parent class over checkstyle's Class Data
 * Abstraction Coupling cap (the parent already references ~29 generated DTO
 * types).
 *
 * <p>The contract under test: any {@code Uuid} field surfaces as a plain CEL
 * string (Kafka's base64url 22-char form), not as a two-key map. Without this
 * normalisation, a rule of the form
 * {@code request.topics.exists(t, t.topicId == "<base64url>")} silently always
 * evaluates to {@code false} — the kind of rule-bypass-by-shape failure mode
 * the engine cannot diagnose at evaluation time. Audit round-5 finding
 * afa1a332.
 */
public class ApiMessageActivationUuidTest {

    @Test
    public void uuidTopicIdSurfacesAsCanonicalString() {
        Uuid id = Uuid.randomUuid();
        MetadataRequestData md = new MetadataRequestData();
        md.topics().add(new MetadataRequestData.MetadataRequestTopic()
            .setName("audit-events").setTopicId(id));

        Map<String, Object> m = ApiMessageActivation.from(md);
        List<?> topics = (List<?>) m.get("topics");
        assertEquals(1, topics.size());
        Map<?, ?> topic = (Map<?, ?>) topics.get(0);
        Object topicId = topic.get("topicId");
        assertTrue(topicId instanceof String,
            "Uuid topicId must surface as String, was "
                + (topicId == null ? "null" : topicId.getClass()));
        assertEquals(id.toString(), topicId);
        // Belt-and-braces: the walker must NOT have descended into Uuid and
        // produced a {mostSignificantBits, leastSignificantBits} map. If it
        // ever does, a rule that previously matched on a string id will
        // silently start evaluating to false.
        assertFalse(topic.containsKey("mostSignificantBits"),
            "Uuid must not be walked into a bits-pair map");
        assertFalse(topic.containsKey("leastSignificantBits"),
            "Uuid must not be walked into a bits-pair map");
    }

    @Test
    public void zeroUuidSurfacesAsItsCanonicalString() {
        // The all-zero sentinel must still normalise to its canonical
        // 22-char base64url form (Uuid.ZERO_UUID.toString) — not to an empty
        // string and not to a bits-pair map. Rule authors writing
        // `request.topics.exists(t, t.topicId == Uuid.ZERO_UUID.toString())`
        // need the broker side of the comparison to match.
        MetadataRequestData md = new MetadataRequestData();
        md.topics().add(new MetadataRequestData.MetadataRequestTopic()
            .setName("zeroed").setTopicId(Uuid.ZERO_UUID));

        Map<String, Object> m = ApiMessageActivation.from(md);
        Map<?, ?> topic = (Map<?, ?>) ((List<?>) m.get("topics")).get(0);
        assertEquals(Uuid.ZERO_UUID.toString(), topic.get("topicId"));
    }

    @Test
    public void uuidNormalisationLetsCelRulesSeeIdAsString() {
        // End-to-end: the natural id-based CEL rule must evaluate true on a
        // request that carries the matching Uuid, and false otherwise. If
        // this regresses, an operator's id-based DENY silently never fires.
        Uuid id = Uuid.randomUuid();
        MetadataRequestData md = new MetadataRequestData();
        md.topics().add(new MetadataRequestData.MetadataRequestTopic()
            .setName("payments").setTopicId(id));

        Map<String, Object> activation = ApiMessageActivation.requestActivation(md);
        CelProgram match = CelCompiler.compile(
            "request.topics.exists(t, t.topicId == \"" + id + "\")");
        assertTrue(match.evalBoolean(activation::get));

        CelProgram nope = CelCompiler.compile(
            "request.topics.exists(t, t.topicId == \"" + Uuid.ZERO_UUID + "\")");
        assertFalse(nope.evalBoolean(activation::get));
    }
}
