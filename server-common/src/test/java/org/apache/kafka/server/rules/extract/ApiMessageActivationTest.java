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

import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.AlterConfigsRequestData.AlterConfigsResource;
import org.apache.kafka.common.message.AlterConfigsRequestData.AlterConfigsResourceCollection;
import org.apache.kafka.common.message.AlterConfigsRequestData.AlterableConfig;
import org.apache.kafka.common.message.AlterConfigsRequestData.AlterableConfigCollection;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData.ScramCredentialUpsertion;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfigCollection;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceDataCollection;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.server.rules.cel.CelCompiler;
import org.apache.kafka.server.rules.cel.CelProgram;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Verifies the generic, reflection-based field extractor works against real
 * generated Kafka data classes. The contract under test:
 *
 *   - top-level scalar fields are reachable by their Java getter name (camelCase)
 *   - nested generated messages are walked recursively
 *   - generated collections (ImplicitLinkedHashMultiCollection etc.) become Lists
 *   - the result composes cleanly with the CEL evaluator so rules like
 *     "request.topics.exists(t, t.name.startsWith(\"audit-\"))" Just Work
 *
 * No hand-coded per-API extractor — that's the whole point.
 */
public class ApiMessageActivationTest {

    // R45-A-1: symmetric defence for CelLimits.STEPS ThreadLocal. See
    // sibling class ApiMessageActivationBytesTest for the rationale.
    @BeforeEach
    public void resetStepBudget() {
        CelProgram.resetEvalStepBudget();
    }

    @AfterEach
    public void leaveCounterClean() {
        CelProgram.resetEvalStepBudget();
    }

    @Test
    public void scalarFieldsAreReachableByGetterName() {
        CreateTopicsRequestData req = new CreateTopicsRequestData()
            .setTimeoutMs(30_000)
            .setValidateOnly(true);
        Map<String, Object> m = ApiMessageActivation.from(req);
        assertEquals(30_000L, ((Number) m.get("timeoutMs")).longValue());
        assertEquals(Boolean.TRUE, m.get("validateOnly"));
    }

    @Test
    public void collectionFieldsBecomeListOfNestedMaps() {
        CreateTopicsRequestData req = new CreateTopicsRequestData();
        CreatableTopicCollection topics = new CreatableTopicCollection();
        topics.add(new CreatableTopic().setName("audit-events").setNumPartitions(8).setReplicationFactor((short) 3));
        topics.add(new CreatableTopic().setName("payments").setNumPartitions(16).setReplicationFactor((short) 3));
        req.setTopics(topics);

        Map<String, Object> m = ApiMessageActivation.from(req);
        Object topicsAny = m.get("topics");
        assertTrue(topicsAny instanceof List, "topics should be a List, was " + (topicsAny == null ? "null" : topicsAny.getClass()));
        List<?> topicMaps = (List<?>) topicsAny;
        assertEquals(2, topicMaps.size());

        Map<?, ?> first = (Map<?, ?>) topicMaps.get(0);
        assertEquals("audit-events", first.get("name"));
        assertEquals(8L, ((Number) first.get("numPartitions")).longValue());
        assertEquals(3L, ((Number) first.get("replicationFactor")).longValue());
    }

    @Test
    public void nestedCollectionsOfNestedMessages() {
        CreatableTopic topic = new CreatableTopic()
            .setName("metrics")
            .setNumPartitions(4);
        CreatableTopicConfigCollection configs = new CreatableTopicConfigCollection();
        configs.add(new CreatableTopicConfig().setName("cleanup.policy").setValue("delete"));
        configs.add(new CreatableTopicConfig().setName("retention.ms").setValue("604800000"));
        topic.setConfigs(configs);

        CreateTopicsRequestData req = new CreateTopicsRequestData();
        CreatableTopicCollection topics = new CreatableTopicCollection();
        topics.add(topic);
        req.setTopics(topics);

        Map<String, Object> m = ApiMessageActivation.from(req);
        List<?> topicMaps = (List<?>) m.get("topics");
        Map<?, ?> topicMap = (Map<?, ?>) topicMaps.get(0);
        List<?> configList = (List<?>) topicMap.get("configs");
        assertEquals(2, configList.size());
        Map<?, ?> firstConfig = (Map<?, ?>) configList.get(0);
        assertEquals("cleanup.policy", firstConfig.get("name"));
        assertEquals("delete", firstConfig.get("value"));
    }

    @Test
    public void integerNumericsAreNormalisedToLong() {
        // CEL's int type is 64-bit. Mixing Integer/Long/Short wrappers in the
        // activation map would force every comparison node to renormalise.
        // Normalise at the boundary instead. timeoutMs is int; replicationFactor
        // on the nested CreatableTopic is short — both must end up as Long.
        CreatableTopicCollection topics = new CreatableTopicCollection();
        topics.add(new CreatableTopic().setName("t").setNumPartitions(16).setReplicationFactor((short) 3));
        CreateTopicsRequestData req = new CreateTopicsRequestData().setTimeoutMs(30_000).setTopics(topics);

        Map<String, Object> m = ApiMessageActivation.from(req);
        assertTrue(m.get("timeoutMs") instanceof Long, "int field should normalise to Long");

        Map<?, ?> firstTopic = (Map<?, ?>) ((List<?>) m.get("topics")).get(0);
        assertTrue(firstTopic.get("numPartitions") instanceof Long, "int field should normalise to Long");
        assertTrue(firstTopic.get("replicationFactor") instanceof Long, "short field should normalise to Long");
    }

    @Test
    public void nullableFieldsSetToNullSurfaceAsNull() {
        // MetadataRequestData.topics is nullable in v1+; the default constructor
        // sets it to an empty list, but we want to verify that an explicit null
        // round-trips through extraction unchanged. CEL rules over nullable
        // fields rely on this — `request.topics == null` must be evaluable.
        MetadataRequestData md = new MetadataRequestData().setTopics(null);
        Map<String, Object> m = ApiMessageActivation.from(md);
        assertTrue(m.containsKey("topics"), "field key must be present even when value is null");
        assertNull(m.get("topics"));
    }

    @Test
    public void emptyCollectionsSurfaceAsEmptyLists() {
        // Default constructor leaves collections as empty lists, not null.
        // The extractor must surface them as empty Java Lists so a CEL
        // expression like `size(request.topics) == 0` evaluates true.
        MetadataRequestData md = new MetadataRequestData();
        Map<String, Object> m = ApiMessageActivation.from(md);
        Object topics = m.get("topics");
        assertTrue(topics instanceof List, "empty collection must surface as a List, was " + topics);
        assertTrue(((List<?>) topics).isEmpty());
    }

    @Test
    public void cacheIsReusedAcrossInvocations() {
        // Reflection is expensive; the per-Class getter set must be looked up once.
        // We assert this indirectly: two activations of the same class share the
        // same internal accessor list (exposed package-private for the test).
        CreateTopicsRequestData req1 = new CreateTopicsRequestData();
        CreateTopicsRequestData req2 = new CreateTopicsRequestData();
        List<ApiMessageActivation.Accessor> a = ApiMessageActivation.accessorsFor(req1.getClass());
        List<ApiMessageActivation.Accessor> b = ApiMessageActivation.accessorsFor(req2.getClass());
        assertSame(a, b, "accessor list must be cached per Class");
    }

    @Test
    public void doesNotExposeObjectOrApiMessageMethods() {
        // We must not surface methods like `getClass`, `hashCode`, `toString`,
        // `apiKey` — those would leak Java implementation details into the
        // rule namespace and create surprising rule semantics.
        Map<String, Object> m = ApiMessageActivation.from(new CreateTopicsRequestData());
        assertFalse(m.containsKey("class"));
        assertFalse(m.containsKey("apiKey"));
        assertFalse(m.containsKey("highestSupportedVersion"));
        assertFalse(m.containsKey("lowestSupportedVersion"));
    }

    @Test
    public void integratesWithCelExpressionOverRequest() {
        // The acceptance-criterion shape: a CEL rule speaks `request.<field>`.
        // Uses requestActivation() — the helper KafkaApis itself calls in
        // production — so the integration test and the production call site
        // share the same activation-shape contract. If the helper ever drops
        // the "request" envelope, this test fails AND the broker silently
        // stops applying request.* rules; updating both at once keeps them
        // in lockstep.
        CreatableTopic topic = new CreatableTopic().setName("audit-events").setNumPartitions(8);
        CreatableTopicCollection topics = new CreatableTopicCollection();
        topics.add(topic);
        CreateTopicsRequestData req = new CreateTopicsRequestData().setTopics(topics);

        Map<String, Object> activation = ApiMessageActivation.requestActivation(req);
        CelProgram prog = CelCompiler.compile("request.topics.exists(t, t.name.startsWith(\"audit-\"))");
        assertTrue(prog.evalBoolean(activation::get));

        CelProgram negative = CelCompiler.compile("request.topics.exists(t, t.name == \"missing\")");
        assertFalse(negative.evalBoolean(activation::get));
    }

    @Test
    public void requestActivationWrapsExtractedMapUnderRequestKey() {
        // Direct unit test for the helper: regardless of what from() produces,
        // requestActivation MUST place it under the single top-level key
        // "request". This is the broker's documented envelope shape (see
        // RuleJsonCodec class javadoc) and the contract a rule author relies
        // on. Locking it down here means callers can't drift the shape.
        CreateTopicsRequestData req = new CreateTopicsRequestData().setTimeoutMs(7_000);

        Map<String, Object> activation = ApiMessageActivation.requestActivation(req);

        assertEquals(1, activation.size(), "envelope must expose exactly one top-level key");
        assertTrue(activation.containsKey("request"), "top-level key must be \"request\"");

        Object inner = activation.get("request");
        assertTrue(inner instanceof Map, "value under \"request\" must be the extracted Map");
        @SuppressWarnings("unchecked")
        Map<String, Object> innerMap = (Map<String, Object>) inner;
        // Numeric scalars are normalised to Long by from(); see that contract.
        assertEquals(7_000L, innerMap.get("timeoutMs"));
    }

    @Test
    public void requestActivationOfNullMessageStillProducesValidEnvelope() {
        // Defensive: even with a null ApiMessage, the helper must produce a
        // map that resolves "request" without NPE — the broker request path
        // must never crash because someone wired a null message into the
        // activation supplier (e.g. a test seam mock). The inner map will be
        // empty, which is the same fail-open posture as from(null).
        Map<String, Object> activation = ApiMessageActivation.requestActivation(null);

        assertEquals(1, activation.size());
        assertTrue(activation.containsKey("request"));
        Object inner = activation.get("request");
        assertTrue(inner instanceof Map);
        assertTrue(((Map<?, ?>) inner).isEmpty(), "from(null) returns the empty map");
    }

    @Test
    public void unknownFieldOnExtractedMapIsNullNotCrash() {
        // CEL rules must be safe against missing fields — they're written
        // against one ApiMessage type but evaluated only when the runtime
        // bitset hits. A typo in a rule must not crash; it must evaluate
        // to null/false. This is enforced on the CelNode side, but the
        // extractor must produce a plain Map that has well-defined
        // .get(missing) semantics (returns null, not throws).
        Map<String, Object> m = ApiMessageActivation.from(new CreateTopicsRequestData());
        assertNotNull(m);
        assertNull(m.get("noSuchField"));
    }

    @Test
    public void nullApiMessageProducesEmptyActivation() {
        // Defensive: the engine should never see a null ApiMessage in practice,
        // but the contract is "no NPE, no surprise" — return an empty map.
        Map<String, Object> m = ApiMessageActivation.from(null);
        assertNotNull(m);
        assertTrue(m.isEmpty());
    }

    @Test
    public void saslAuthBytesAreNeverSurfacedToCel() {
        // Codex deep-audit P1: the generic reflection walk surfaces every field
        // that has a public zero-arg accessor. SaslAuthenticateRequestData.authBytes
        // is one such — its value is the raw SASL bytes exchanged during the
        // handshake. Exposing it via the activation map would let an operator
        // with rule-write access exfiltrate credentials by writing predicates
        // that compare authBytes against constants and watching the audit log.
        // The walker has no way to tell benign rules from hostile ones, so it
        // must simply never put this field in the map.
        SaslAuthenticateRequestData req = new SaslAuthenticateRequestData()
            .setAuthBytes(new byte[]{0, 'a', 'd', 'm', 'i', 'n', 0, 's', '3', 'c', 'r', 'e', 't'});
        Map<String, Object> m = ApiMessageActivation.from(req);
        assertFalse(m.containsKey("authBytes"),
            "authBytes must NEVER appear in the activation map — credential exfiltration vector");
    }

    @Test
    public void scramSaltAndSaltedPasswordAreNeverSurfacedToCel() {
        // Codex deep-audit P1 part 2: the SCRAM upsertion carries both `salt`
        // and `saltedPassword`. Either is sufficient for an offline credential
        // attack against the target user. Both must be redacted from the
        // activation map. The non-sensitive fields (name, mechanism, iterations)
        // remain visible so a rule like
        //   request.upsertions.exists(u, u.iterations < 4096)
        // can still enforce a minimum iteration count.
        ScramCredentialUpsertion ups = new ScramCredentialUpsertion()
            .setName("alice")
            .setMechanism((byte) 1)
            .setIterations(8192)
            .setSalt(new byte[]{1, 2, 3, 4})
            .setSaltedPassword(new byte[]{5, 6, 7, 8});
        AlterUserScramCredentialsRequestData req = new AlterUserScramCredentialsRequestData()
            .setUpsertions(java.util.Collections.singletonList(ups));

        Map<String, Object> m = ApiMessageActivation.from(req);
        List<?> upsertionList = (List<?>) m.get("upsertions");
        assertEquals(1, upsertionList.size());
        Map<?, ?> upsertionMap = (Map<?, ?>) upsertionList.get(0);

        assertFalse(upsertionMap.containsKey("salt"),
            "SCRAM salt must NEVER appear in the activation map");
        assertFalse(upsertionMap.containsKey("saltedPassword"),
            "SCRAM saltedPassword must NEVER appear in the activation map");
        // Non-sensitive fields must remain reachable.
        assertEquals("alice", upsertionMap.get("name"));
        assertEquals(1L, upsertionMap.get("mechanism"));
        assertEquals(8192L, upsertionMap.get("iterations"));
    }

    @Test
    public void recursionDepthIsBoundedAgainstPathologicalNesting() {
        // Codex deep-audit P1 part 3 (round-23 #184 posture flip): the
        // reflection walk recurses into any object with at least one accessor.
        // Kafka's generated DTOs don't contain cycles, so this isn't reachable
        // from a well-formed protocol message — but a hostile request or a
        // future protocol with deep nesting we never anticipated must not be
        // able to blow the stack or spin the walker forever. MAX_DEPTH=32 caps
        // the walk.
        //
        // <p><b>Previously:</b> toMap() returned an empty map on depth
        // exhaustion (soft truncation, fail-OPEN). That created an attacker
        // primitive: a DENY rule keyed on a field that sits below depth 32 on
        // a hostile Message chain silently evaluated against {} and never
        // matched, evading the rule.
        //
        // <p><b>Now (round-17 MED #184 / round-23 fix):</b> toMap throws
        // {@link ActivationBudgetExceededException} on depth exhaustion,
        // symmetric with {@link convertIterable} which has thrown on the same
        // condition since round-15 H1. The engine catches that typed signal
        // separately from the generic Throwable branch and fails the request
        // CLOSED (synthetic POLICY_VIOLATION DENY).
        //
        // A self-referencing test class is the simplest way to demonstrate
        // the cap: without the depth bound, toMap() would never return.
        SelfReferencingNode root = new SelfReferencingNode();
        ActivationBudgetExceededException ex = assertThrows(
            ActivationBudgetExceededException.class,
            () -> ApiMessageActivation.from(root));
        assertTrue(
            ex.getMessage().contains("depth limit of " + ApiMessageActivation.MAX_DEPTH),
            "expected depth-limit error, got: " + ex.getMessage());
        assertTrue(
            ex.getMessage().contains(SelfReferencingNode.class.getName()),
            "depth-limit error must name the offending walker class for forensics; got: "
                + ex.getMessage());
    }

    /**
     * Self-referencing fixture used only by {@link #recursionDepthIsBoundedAgainstPathologicalNesting}.
     * Public no-arg getter named {@code child} returning {@code this} produces
     * an unbounded recursive walk in the pre-fix code. Depth-bounded code
     * terminates at MAX_DEPTH.
     */
    @SuppressWarnings("unused")
    public static final class SelfReferencingNode implements org.apache.kafka.common.protocol.ApiMessage {
        public SelfReferencingNode child() {
            return this;
        }
        @Override public short apiKey() {
            return -1;
        }
        @Override public short lowestSupportedVersion() {
            return 0;
        }
        @Override public short highestSupportedVersion() {
            return 0;
        }
        @Override public org.apache.kafka.common.protocol.Message duplicate() {
            return new SelfReferencingNode();
        }
        @Override public java.util.List<org.apache.kafka.common.protocol.types.RawTaggedField> unknownTaggedFields() {
            return java.util.Collections.emptyList();
        }
        @Override public void read(org.apache.kafka.common.protocol.Readable readable, short version) {
        }
        @Override public void write(org.apache.kafka.common.protocol.Writable writable,
                                    org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                    short version) {
        }
        @Override public int size(org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                  short version) {
            return 0;
        }
        @Override public void addSize(org.apache.kafka.common.protocol.MessageSizeAccumulator size,
                                      org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                      short version) {
        }
    }

    @Test
    public void accessorInvocationBudgetTerminatesPathologicalWideWalk() {
        // Codex deep-audit P1b fix: the depth cap alone is not enough. A
        // shallow but extremely wide message can still perform millions of
        // accessor invocations before the depth cap kicks in at each branch.
        // The total-invocation budget bounds the aggregate work and raises
        // ActivationBudgetExceededException long before the request thread is
        // starved. The engine catches that specific type ahead of the generic
        // Throwable branch and fails the request CLOSED (synthetic
        // POLICY_VIOLATION DENY), since budget overflow is attacker-shaped
        // and the cap is what makes worst-case walk cost bounded — see
        // ActivationBudgetExceededException javadoc.
        //
        // <p><b>Round-23 #184 fixture refactor:</b> the previous shape used
        // 1000 SelfReferencingNode children to inflate accessor invocations
        // depth-times-width-style. After round-23 promoted toMap's depth-cap
        // to fail-CLOSED (#184), the very first SelfReferencingNode child
        // throws on depth limit at ~30 invocations — well below
        // MAX_ACCESSOR_INVOCATIONS=10_000 — so the assertion that names
        // "accessor budget" would no longer hold. A wide flat scalar list
        // is the cleanest fixture: the per-iteration bump inside
        // convertIterable charges each element (Codex deep-audit P1d), and
        // 11_000 strings overflow the budget at element 10_001 without ever
        // descending past depth 1.
        WideNode root = new WideNode();
        java.util.List<String> kids = new java.util.ArrayList<>(11_000);
        for (int n = 0; n < 11_000; n++) {
            kids.add("k" + n);
        }
        root.children = kids;
        ActivationBudgetExceededException ex = assertThrows(
            ActivationBudgetExceededException.class,
            () -> ApiMessageActivation.from(root));
        assertTrue(ex.getMessage().contains("accessor budget"),
            "expected accessor-budget error, got: " + ex.getMessage());
    }

    /**
     * Fixture used only by {@link #accessorInvocationBudgetTerminatesPathologicalWideWalk}.
     * Exposes a wide list of String elements; the per-element bump inside
     * {@code convertIterable} charges each scalar against
     * {@code MAX_ACCESSOR_INVOCATIONS}, so a sufficiently large list overflows
     * the budget regardless of depth.
     */
    @SuppressWarnings("unused")
    public static final class WideNode implements org.apache.kafka.common.protocol.ApiMessage {
        public java.util.List<String> children = java.util.Collections.emptyList();
        public java.util.List<String> children() {
            return children;
        }
        @Override public short apiKey() {
            return -1;
        }
        @Override public short lowestSupportedVersion() {
            return 0;
        }
        @Override public short highestSupportedVersion() {
            return 0;
        }
        @Override public org.apache.kafka.common.protocol.Message duplicate() {
            return new WideNode();
        }
        @Override public java.util.List<org.apache.kafka.common.protocol.types.RawTaggedField> unknownTaggedFields() {
            return java.util.Collections.emptyList();
        }
        @Override public void read(org.apache.kafka.common.protocol.Readable readable, short version) {
        }
        @Override public void write(org.apache.kafka.common.protocol.Writable writable,
                                    org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                    short version) {
        }
        @Override public int size(org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                  short version) {
            return 0;
        }
        @Override public void addSize(org.apache.kafka.common.protocol.MessageSizeAccumulator size,
                                      org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                      short version) {
        }
    }

    @Test
    public void iterableElementBudgetTerminatesGiantFlatScalarList() {
        // Codex deep-audit P1d fix: the per-accessor budget alone does NOT
        // bound a wide, flat repeated scalar field. convertIterable iterates
        // every element and, for scalars (Long, String, byte[], etc.),
        // convertScalar returns without touching `invocations`. An attacker
        // who can pack a giant repeated scalar into a max-size request
        // (e.g. millions of partition ids in a request that legitimately
        // takes a partition-ids array) gets an unbudgeted O(N) list copy.
        // Counting each iterated element against the same
        // MAX_ACCESSOR_INVOCATIONS=10_000 budget fixes this — and this test
        // pins the fix by feeding a 20_000-long Long list into a fixture and
        // asserting the budget aborts the walk.
        WideScalarNode root = new WideScalarNode();
        java.util.List<Long> ids = new java.util.ArrayList<>();
        for (long i = 0; i < 20_000; i++) {
            ids.add(i);
        }
        root.ids = ids;
        ActivationBudgetExceededException ex = assertThrows(
            ActivationBudgetExceededException.class,
            () -> ApiMessageActivation.from(root));
        assertTrue(ex.getMessage().contains("accessor budget"),
            "expected accessor-budget error, got: " + ex.getMessage());
    }

    /**
     * Fixture for {@link #iterableElementBudgetTerminatesGiantFlatScalarList}.
     * Exposes a single field of type {@code List<Long>} — the element type is
     * a scalar so each item goes through {@code convertScalar()}, exercising
     * the iterable-element budget specifically (not the per-accessor one,
     * which was already covered by {@link WideNode}).
     */
    @SuppressWarnings("unused")
    public static final class WideScalarNode implements org.apache.kafka.common.protocol.ApiMessage {
        public java.util.List<Long> ids = java.util.Collections.emptyList();
        public java.util.List<Long> ids() {
            return ids;
        }
        @Override public short apiKey() {
            return -1;
        }
        @Override public short lowestSupportedVersion() {
            return 0;
        }
        @Override public short highestSupportedVersion() {
            return 0;
        }
        @Override public org.apache.kafka.common.protocol.Message duplicate() {
            return new WideScalarNode();
        }
        @Override public java.util.List<org.apache.kafka.common.protocol.types.RawTaggedField> unknownTaggedFields() {
            return java.util.Collections.emptyList();
        }
        @Override public void read(org.apache.kafka.common.protocol.Readable readable, short version) {
        }
        @Override public void write(org.apache.kafka.common.protocol.Writable writable,
                                    org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                    short version) {
        }
        @Override public int size(org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                  short version) {
            return 0;
        }
        @Override public void addSize(org.apache.kafka.common.protocol.MessageSizeAccumulator size,
                                      org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                      short version) {
        }
    }

    @Test
    public void chainedNestedIterablesAreCappedAtMaxDepth() {
        // Round-15 Walker HIGH H1: depth bound must apply to iterable chains.
        // Before the fix, convertIterable recursed back through convert() at
        // the SAME depth (because depth only incremented in toMap's Message
        // descent). A pure Iterable<Iterable<Iterable<...>>> chain with no
        // Messages at the leaves bypassed MAX_DEPTH entirely. The
        // per-element accessor-budget bump bounds aggregate work but does
        // NOT bound stack depth — a single-element chain N deep blows the
        // JVM stack (each frame ~100B, default 512KB stack ~ 5..10k frames)
        // well before MAX_ACCESSOR_INVOCATIONS=10_000 fires, since the
        // budget only ticks ONCE per single-element iterable.
        //
        // This test builds a single-element nested-iterable chain N levels
        // deep where N is well past MAX_DEPTH but well below the accessor
        // budget. The walk must terminate with a typed
        // ActivationBudgetExceededException (fail-CLOSED) — the same signal
        // RuleEngine catches separately from the generic Throwable branch —
        // rather than recurse until the JVM stack dies.
        int chainDepth = ApiMessageActivation.MAX_DEPTH * 4;
        IterableChainNode root = new IterableChainNode(chainDepth);
        ActivationBudgetExceededException ex = assertThrows(
            ActivationBudgetExceededException.class,
            () -> ApiMessageActivation.from(root));
        assertTrue(ex.getMessage().contains("depth limit of " + ApiMessageActivation.MAX_DEPTH),
            "expected depth-limit error from convertIterable, got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("walking iterable"),
            "message must localise the failure to the iterable path, got: " + ex.getMessage());
    }

    /**
     * Fixture for {@link #chainedNestedIterablesAreCappedAtMaxDepth}. The
     * single accessor {@code chain()} returns a {@code List<Object>} where
     * the sole element is either another {@code List<Object>} (deeper
     * nesting) or the empty list (leaf). With {@code depth} far above
     * {@link ApiMessageActivation#MAX_DEPTH}, the walker must throw on the
     * iterable-depth guard before recursing into the deepest leaf.
     */
    @SuppressWarnings("unused")
    public static final class IterableChainNode implements org.apache.kafka.common.protocol.ApiMessage {
        private final int chainDepth;
        public IterableChainNode(int chainDepth) {
            this.chainDepth = chainDepth;
        }
        public java.util.List<Object> chain() {
            return buildChain(chainDepth);
        }
        private static java.util.List<Object> buildChain(int remaining) {
            if (remaining <= 0) {
                return java.util.Collections.emptyList();
            }
            java.util.List<Object> outer = new java.util.ArrayList<>(1);
            outer.add(buildChain(remaining - 1));
            return outer;
        }
        @Override public short apiKey() {
            return -1;
        }
        @Override public short lowestSupportedVersion() {
            return 0;
        }
        @Override public short highestSupportedVersion() {
            return 0;
        }
        @Override public org.apache.kafka.common.protocol.Message duplicate() {
            return new IterableChainNode(chainDepth);
        }
        @Override public java.util.List<org.apache.kafka.common.protocol.types.RawTaggedField> unknownTaggedFields() {
            return java.util.Collections.emptyList();
        }
        @Override public void read(org.apache.kafka.common.protocol.Readable readable, short version) {
        }
        @Override public void write(org.apache.kafka.common.protocol.Writable writable,
                                    org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                    short version) {
        }
        @Override public int size(org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                  short version) {
            return 0;
        }
        @Override public void addSize(org.apache.kafka.common.protocol.MessageSizeAccumulator size,
                                      org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                      short version) {
        }
    }

    @Test
    public void shallowNestedIterableOfMessagesStillWalksCleanly() {
        // Round-15 Walker HIGH H1: the depth-on-iterable bump must not
        // regress legitimate request shapes. The deepest production walk
        // observed across the Kafka request surface is roughly:
        //   Request -> Iterable -> Message -> Iterable -> Message -> scalar
        // i.e. <= 2 iterable layers. The bump consumes one depth slot per
        // iterable layer (was zero), but MAX_DEPTH=32 leaves ample headroom.
        // This test pins that the canonical CreateTopics shape (2 iterable
        // layers + 2 message layers) walks to completion without raising
        // any budget exception.
        CreatableTopic topic = new CreatableTopic()
            .setName("legit-topic")
            .setNumPartitions(4);
        CreatableTopicConfigCollection configs = new CreatableTopicConfigCollection();
        configs.add(new CreatableTopicConfig().setName("cleanup.policy").setValue("compact"));
        topic.setConfigs(configs);
        CreatableTopicCollection topics = new CreatableTopicCollection();
        topics.add(topic);
        CreateTopicsRequestData req = new CreateTopicsRequestData().setTopics(topics);

        Map<String, Object> m = ApiMessageActivation.from(req);
        List<?> topicList = (List<?>) m.get("topics");
        Map<?, ?> topicMap = (Map<?, ?>) topicList.get(0);
        assertEquals("legit-topic", topicMap.get("name"));
        List<?> configList = (List<?>) topicMap.get("configs");
        Map<?, ?> configMap = (Map<?, ?>) configList.get(0);
        assertEquals("cleanup.policy", configMap.get("name"));
        assertEquals("compact", configMap.get("value"));
    }

    @Test
    public void sensitiveAlterConfigsValueIsRedactedWhenNameMatchesPasswordPattern() {
        // Codex/Gemini final-audit P1#6: the SENSITIVE_NAMES flat denylist cannot
        // redact AlterableConfig.value — the getter is just called `value`, and
        // most configs (retention.ms, cleanup.policy, …) need to remain visible
        // so legitimate operator rules can filter on them. Sensitivity is
        // contextual: when the sibling `name` is a known credential key
        // (ssl.*.password, sasl.jaas.config, *secret*) the value MUST be
        // redacted, because a rule like
        //   request.resources.exists(r, r.configs.exists(c,
        //       c.name == "ssl.keystore.password" && c.value.startsWith("guess")))
        // would otherwise be a per-character credential exfiltration oracle.
        AlterableConfigCollection configs = new AlterableConfigCollection();
        configs.add(new AlterableConfig().setName("ssl.keystore.password").setValue("super-secret-pass"));
        configs.add(new AlterableConfig().setName("retention.ms").setValue("604800000"));
        AlterConfigsResource resource = new AlterConfigsResource()
            .setResourceType((byte) 2)
            .setResourceName("audit-events")
            .setConfigs(configs);
        AlterConfigsResourceCollection resources = new AlterConfigsResourceCollection();
        resources.add(resource);
        AlterConfigsRequestData req = new AlterConfigsRequestData().setResources(resources);

        Map<String, Object> m = ApiMessageActivation.from(req);
        List<?> resourceList = (List<?>) m.get("resources");
        Map<?, ?> resourceMap = (Map<?, ?>) resourceList.get(0);
        List<?> configList = (List<?>) resourceMap.get("configs");
        assertEquals(2, configList.size());

        Map<?, ?> sensitiveCfg = (Map<?, ?>) configList.get(0);
        assertEquals("ssl.keystore.password", sensitiveCfg.get("name"));
        assertTrue(sensitiveCfg.containsKey("value"),
            "value key MUST remain present (so a hostile rule can't probe `c.value == null` to learn redaction state); only the value itself is nulled out");
        assertNull(sensitiveCfg.get("value"),
            "value MUST be null for credential-bearing config keys — exfiltration vector");

        Map<?, ?> benignCfg = (Map<?, ?>) configList.get(1);
        assertEquals("retention.ms", benignCfg.get("name"));
        assertEquals("604800000", benignCfg.get("value"),
            "non-credential config values must remain visible so legitimate rules can filter on them");
    }

    @Test
    public void alterConfigsValueIsRedactedWhenSiblingNameIsNull() {
        // Round-10 audit (LOW, defence-in-depth): if a future schema revision
        // adds nullableVersions to AlterableConfig.Name, or an in-process
        // caller constructs an instance with setName(null), the prior
        // `name instanceof String && isSensitive(name)` check would silently
        // fail-OPEN — the predicate is false for null, redaction skipped,
        // value visible. The fix biases to redact: any non-String shape
        // means we cannot prove the name is benign, so we redact.
        AlterableConfigCollection configs = new AlterableConfigCollection();
        configs.add(new AlterableConfig().setName(null).setValue("could-be-any-secret"));
        AlterConfigsResource resource = new AlterConfigsResource()
            .setResourceType((byte) 2)
            .setResourceName("audit-events")
            .setConfigs(configs);
        AlterConfigsResourceCollection resources = new AlterConfigsResourceCollection();
        resources.add(resource);
        AlterConfigsRequestData req = new AlterConfigsRequestData().setResources(resources);

        Map<String, Object> m = ApiMessageActivation.from(req);
        List<?> resourceList = (List<?>) m.get("resources");
        Map<?, ?> resourceMap = (Map<?, ?>) resourceList.get(0);
        List<?> configList = (List<?>) resourceMap.get("configs");
        Map<?, ?> cfg = (Map<?, ?>) configList.get(0);
        assertTrue(cfg.containsKey("value"),
            "value key MUST remain present so a rule can't probe `c.value == null` to learn redaction state");
        assertNull(cfg.get("value"),
            "null sibling name MUST trigger redact-by-default — we cannot prove the name is benign, so we redact");
    }

    @Test
    public void alterConfigsValueIsRedactedWhenSiblingNameIsEmptyOrBlank() {
        // Round-20 HIGH C-1: redact-by-default must extend to empty and
        // whitespace-only sibling names. None of the credential patterns
        // in isSensitiveConfigName matches an empty string or a
        // whitespace-only string — ".password", ".keystore.key",
        // "secret" and the listener-prefixed forms all require at least
        // some non-whitespace prefix or contained substring. With the
        // pre-fix predicate `name instanceof String && isSensitive(name)`,
        // an instance constructed via `setName("").setValue(...)` or
        // `setName("   ").setValue(...)` was treated as a benign config
        // and its value left visible. No legitimate Kafka config is keyed
        // by an empty or whitespace-only string, so this shape can only
        // arise from a corrupted wire frame or an in-process caller —
        // either way, we cannot prove the name is benign and must redact.
        //
        // Pin every shape that String.isBlank() classifies as blank
        // (empty + ASCII whitespace). The map key must remain so a hostile
        // rule cannot probe `c.value == null` to discover the redaction
        // state (same posture as the null-name branch above).
        //
        // NOTE: String.isBlank() uses Character.isWhitespace, which excludes
        // non-breaking spaces (NBSP U+00A0, FIGURE SPACE U+2007, NARROW
        // NBSP U+202F). A name composed solely of those codepoints is NOT
        // caught by this fix. R20-C scope is empty/ASCII-blank; wider
        // Unicode-blank coverage would need a helper mirroring
        // RuleEngine#isAllWhitespace (which unions isWhitespace +
        // isSpaceChar). Defer unless re-flagged.
        String[] blankShapes = {
            "",         // empty
            "   ",      // ASCII spaces
            "\t",       // ASCII tab
            "\n",       // ASCII newline
            " \t\n "    // mixed whitespace
        };
        for (String blank : blankShapes) {
            AlterableConfigCollection configs = new AlterableConfigCollection();
            configs.add(new AlterableConfig().setName(blank).setValue("could-be-any-secret"));
            AlterConfigsResource resource = new AlterConfigsResource()
                .setResourceType((byte) 2)
                .setResourceName("audit-events")
                .setConfigs(configs);
            AlterConfigsResourceCollection resources = new AlterConfigsResourceCollection();
            resources.add(resource);
            AlterConfigsRequestData req = new AlterConfigsRequestData().setResources(resources);

            Map<String, Object> m = ApiMessageActivation.from(req);
            List<?> resourceList = (List<?>) m.get("resources");
            Map<?, ?> resourceMap = (Map<?, ?>) resourceList.get(0);
            List<?> configList = (List<?>) resourceMap.get("configs");
            Map<?, ?> cfg = (Map<?, ?>) configList.get(0);
            assertTrue(cfg.containsKey("value"),
                "value key MUST remain present for blank shape " + describe(blank)
                    + " so a rule can't probe `c.value == null` to learn redaction state");
            assertNull(cfg.get("value"),
                "blank sibling name " + describe(blank)
                    + " MUST trigger redact-by-default — we cannot prove the name is benign, so we redact");
        }
    }

    private static String describe(String s) {
        StringBuilder out = new StringBuilder("\"");
        for (int i = 0; i < s.length(); ) {
            int cp = s.codePointAt(i);
            if (cp >= 0x20 && cp < 0x7F) {
                out.appendCodePoint(cp);
            } else {
                out.append(String.format("\\u%04X", cp));
            }
            i += Character.charCount(cp);
        }
        return out.append("\"").toString();
    }

    @Test
    public void sensitiveIncrementalAlterConfigsValueIsRedactedWhenNameMatchesPasswordPattern() {
        // Same contract as AlterConfigsRequest but exercised through the
        // distinct generated class IncrementalAlterConfigsRequestData$AlterableConfig.
        // Both classes are listed in CONFIG_PAIR_CLASS_NAMES; pinning both here
        // prevents one of the two being silently dropped on a future refactor.
        IncrementalAlterConfigsRequestData.AlterableConfigCollection configs =
            new IncrementalAlterConfigsRequestData.AlterableConfigCollection();
        configs.add(new IncrementalAlterConfigsRequestData.AlterableConfig()
            .setName("sasl.jaas.config")
            .setConfigOperation((byte) 0)
            .setValue("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"hunter2\";"));
        configs.add(new IncrementalAlterConfigsRequestData.AlterableConfig()
            .setName("retention.bytes")
            .setConfigOperation((byte) 0)
            .setValue("1073741824"));
        IncrementalAlterConfigsRequestData.AlterConfigsResource resource =
            new IncrementalAlterConfigsRequestData.AlterConfigsResource()
                .setResourceType((byte) 2)
                .setResourceName("audit-events")
                .setConfigs(configs);
        IncrementalAlterConfigsRequestData.AlterConfigsResourceCollection resources =
            new IncrementalAlterConfigsRequestData.AlterConfigsResourceCollection();
        resources.add(resource);
        IncrementalAlterConfigsRequestData req = new IncrementalAlterConfigsRequestData()
            .setResources(resources);

        Map<String, Object> m = ApiMessageActivation.from(req);
        List<?> resourceList = (List<?>) m.get("resources");
        Map<?, ?> resourceMap = (Map<?, ?>) resourceList.get(0);
        List<?> configList = (List<?>) resourceMap.get("configs");

        Map<?, ?> jaas = (Map<?, ?>) configList.get(0);
        assertEquals("sasl.jaas.config", jaas.get("name"));
        assertNull(jaas.get("value"),
            "sasl.jaas.config MUST be redacted — its module-options string carries the SASL principal's password");

        Map<?, ?> retention = (Map<?, ?>) configList.get(1);
        assertEquals("retention.bytes", retention.get("name"));
        assertEquals("1073741824", retention.get("value"));
    }

    @Test
    public void sensitiveCreateTopicsConfigValueIsRedactedWhenNameMatchesPasswordPattern() {
        // CreateTopicsRequest carries inline per-topic config overrides via
        // CreatableTopicConfig. A topic created with `ssl.keystore.password=...`
        // inline must redact the value through the same contextual path; the
        // value field name (`value`) is identical to the AlterConfigs case.
        // This is the third class enumerated in CONFIG_PAIR_CLASS_NAMES.
        CreatableTopicConfigCollection configs = new CreatableTopicConfigCollection();
        configs.add(new CreatableTopicConfig().setName("ssl.truststore.password").setValue("trust-me-bro"));
        configs.add(new CreatableTopicConfig().setName("cleanup.policy").setValue("compact"));
        CreatableTopic topic = new CreatableTopic()
            .setName("audit-events")
            .setNumPartitions(8)
            .setReplicationFactor((short) 3)
            .setConfigs(configs);
        CreatableTopicCollection topics = new CreatableTopicCollection();
        topics.add(topic);
        CreateTopicsRequestData req = new CreateTopicsRequestData().setTopics(topics);

        Map<String, Object> m = ApiMessageActivation.from(req);
        Map<?, ?> topicMap = (Map<?, ?>) ((List<?>) m.get("topics")).get(0);
        List<?> configList = (List<?>) topicMap.get("configs");

        Map<?, ?> sensitiveCfg = (Map<?, ?>) configList.get(0);
        assertEquals("ssl.truststore.password", sensitiveCfg.get("name"));
        assertNull(sensitiveCfg.get("value"),
            "value MUST be null when name is *.password");

        Map<?, ?> benignCfg = (Map<?, ?>) configList.get(1);
        assertEquals("cleanup.policy", benignCfg.get("name"));
        assertEquals("compact", benignCfg.get("value"));
    }

    @Test
    public void operatorDefinedSecretSuffixIsAlsoRedacted() {
        // The pattern is intentionally broad: operators routinely define
        // custom configs with names like `my.app.client.secret` or
        // `tenant.shared.secret`. The `.secret` substring match catches these
        // by convention without us needing to maintain an exhaustive list.
        // Over-redacting an unfortunately-named non-credential config is a
        // small CEL-rule inconvenience; under-redacting a real secret is an
        // incident. Bias to redact.
        AlterableConfigCollection configs = new AlterableConfigCollection();
        configs.add(new AlterableConfig().setName("my.app.client.secret").setValue("c0ffeebabe"));
        AlterConfigsResource resource = new AlterConfigsResource()
            .setResourceType((byte) 2)
            .setResourceName("audit-events")
            .setConfigs(configs);
        AlterConfigsResourceCollection resources = new AlterConfigsResourceCollection();
        resources.add(resource);
        AlterConfigsRequestData req = new AlterConfigsRequestData().setResources(resources);

        Map<String, Object> m = ApiMessageActivation.from(req);
        List<?> configList = (List<?>) ((Map<?, ?>) ((List<?>) m.get("resources")).get(0)).get("configs");
        Map<?, ?> cfg = (Map<?, ?>) configList.get(0);
        assertNull(cfg.get("value"),
            "operator-defined *.secret names must be redacted by the `secret` substring match");
    }

    @Test
    public void listenerPrefixedSaslJaasConfigIsAlsoRedacted() {
        // Codex round-2 audit finding: Kafka permits per-listener-and-mechanism
        // overrides such as `listener.name.internal.scram-sha-256.sasl.jaas.config`
        // (see ListenerName / BrokerSecurityConfigs); the prefixed form holds the
        // exact same SASL principal password as the bare `sasl.jaas.config` key
        // and MUST be redacted with the same care. A naive `equals("sasl.jaas.config")`
        // match misses these and leaks credentials. The fix is `endsWith(".sasl.jaas.config")`.
        AlterableConfigCollection configs = new AlterableConfigCollection();
        configs.add(new AlterableConfig()
            .setName("listener.name.internal.scram-sha-256.sasl.jaas.config")
            .setValue("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"broker\" password=\"hunter2\";"));
        AlterConfigsResource resource = new AlterConfigsResource()
            .setResourceType((byte) 2)
            .setResourceName("audit-events")
            .setConfigs(configs);
        AlterConfigsResourceCollection resources = new AlterConfigsResourceCollection();
        resources.add(resource);
        AlterConfigsRequestData req = new AlterConfigsRequestData().setResources(resources);

        Map<String, Object> m = ApiMessageActivation.from(req);
        Map<?, ?> cfg = (Map<?, ?>) ((List<?>) ((Map<?, ?>) ((List<?>) m.get("resources")).get(0)).get("configs")).get(0);
        assertEquals("listener.name.internal.scram-sha-256.sasl.jaas.config", cfg.get("name"));
        assertNull(cfg.get("value"),
            "listener-prefixed sasl.jaas.config carries the SASL principal password and must redact");
    }

    @Test
    public void listenerPrefixedKeystorePasswordIsAlsoRedacted() {
        // Listener-prefixed *.password configs (eg
        // `listener.name.external.ssl.keystore.password`) are real Kafka
        // credential keys. The existing `.password` suffix match already
        // catches them — this test pins that behaviour so a future
        // narrowing of the pattern cannot regress silently.
        AlterableConfigCollection configs = new AlterableConfigCollection();
        configs.add(new AlterableConfig()
            .setName("listener.name.external.ssl.keystore.password")
            .setValue("trust-me-bro"));
        AlterConfigsResource resource = new AlterConfigsResource()
            .setResourceType((byte) 2)
            .setResourceName("audit-events")
            .setConfigs(configs);
        AlterConfigsResourceCollection resources = new AlterConfigsResourceCollection();
        resources.add(resource);
        AlterConfigsRequestData req = new AlterConfigsRequestData().setResources(resources);

        Map<String, Object> m = ApiMessageActivation.from(req);
        Map<?, ?> cfg = (Map<?, ?>) ((List<?>) ((Map<?, ?>) ((List<?>) m.get("resources")).get(0)).get("configs")).get(0);
        assertNull(cfg.get("value"),
            "listener-prefixed keystore.password must redact via the .password suffix match");
    }

    @Test
    public void sslKeystoreCertificateChainIsRedacted() {
        // Codex round-3 P2: ssl.keystore.certificate.chain is declared
        // ConfigDef.Type.PASSWORD in SslConfigs alongside ssl.keystore.key.
        // Upstream broker config logging redacts it; the rule engine MUST
        // not surface it via the activation map either, or a CEL rule
        // could read PEM material that upstream redaction explicitly hides.
        AlterableConfigCollection configs = new AlterableConfigCollection();
        configs.add(new AlterableConfig()
            .setName("ssl.keystore.certificate.chain")
            .setValue("-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----"));
        AlterConfigsResource resource = new AlterConfigsResource()
            .setResourceType((byte) 2)
            .setResourceName("audit-events")
            .setConfigs(configs);
        AlterConfigsResourceCollection resources = new AlterConfigsResourceCollection();
        resources.add(resource);
        AlterConfigsRequestData req = new AlterConfigsRequestData().setResources(resources);

        Map<String, Object> m = ApiMessageActivation.from(req);
        Map<?, ?> cfg = (Map<?, ?>) ((List<?>) ((Map<?, ?>) ((List<?>) m.get("resources")).get(0)).get("configs")).get(0);
        assertEquals("ssl.keystore.certificate.chain", cfg.get("name"));
        assertNull(cfg.get("value"),
            "ssl.keystore.certificate.chain is PASSWORD-typed in SslConfigs and must be redacted");
    }

    @Test
    public void listenerPrefixedSslKeystoreCertificateChainIsRedacted() {
        // Codex round-3 P2: per-listener override of the keystore PEM chain
        // (eg. `listener.name.internal.ssl.keystore.certificate.chain`)
        // holds the same PEM material as the bare key and must redact the
        // same way. Pin the endsWith match against a regression.
        AlterableConfigCollection configs = new AlterableConfigCollection();
        configs.add(new AlterableConfig()
            .setName("listener.name.internal.ssl.keystore.certificate.chain")
            .setValue("-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----"));
        AlterConfigsResource resource = new AlterConfigsResource()
            .setResourceType((byte) 2)
            .setResourceName("audit-events")
            .setConfigs(configs);
        AlterConfigsResourceCollection resources = new AlterConfigsResourceCollection();
        resources.add(resource);
        AlterConfigsRequestData req = new AlterConfigsRequestData().setResources(resources);

        Map<String, Object> m = ApiMessageActivation.from(req);
        Map<?, ?> cfg = (Map<?, ?>) ((List<?>) ((Map<?, ?>) ((List<?>) m.get("resources")).get(0)).get("configs")).get(0);
        assertNull(cfg.get("value"),
            "listener-prefixed ssl.keystore.certificate.chain must redact via the endsWith match");
    }

    @Test
    public void sslTruststoreCertificatesIsRedacted() {
        // Codex round-3 P2: ssl.truststore.certificates is also
        // ConfigDef.Type.PASSWORD in SslConfigs. Same reasoning as the
        // keystore chain: upstream treats it opaquely, so CEL must too.
        AlterableConfigCollection configs = new AlterableConfigCollection();
        configs.add(new AlterableConfig()
            .setName("ssl.truststore.certificates")
            .setValue("-----BEGIN CERTIFICATE-----\nMIIC...\n-----END CERTIFICATE-----"));
        AlterConfigsResource resource = new AlterConfigsResource()
            .setResourceType((byte) 2)
            .setResourceName("audit-events")
            .setConfigs(configs);
        AlterConfigsResourceCollection resources = new AlterConfigsResourceCollection();
        resources.add(resource);
        AlterConfigsRequestData req = new AlterConfigsRequestData().setResources(resources);

        Map<String, Object> m = ApiMessageActivation.from(req);
        Map<?, ?> cfg = (Map<?, ?>) ((List<?>) ((Map<?, ?>) ((List<?>) m.get("resources")).get(0)).get("configs")).get(0);
        assertNull(cfg.get("value"),
            "ssl.truststore.certificates is PASSWORD-typed in SslConfigs and must be redacted");
    }

    @Test
    public void listenerPrefixedSslTruststoreCertificatesIsRedacted() {
        // Codex round-3 P2: same listener-prefixed pattern for truststore PEM.
        AlterableConfigCollection configs = new AlterableConfigCollection();
        configs.add(new AlterableConfig()
            .setName("listener.name.external.ssl.truststore.certificates")
            .setValue("-----BEGIN CERTIFICATE-----\nMIIC...\n-----END CERTIFICATE-----"));
        AlterConfigsResource resource = new AlterConfigsResource()
            .setResourceType((byte) 2)
            .setResourceName("audit-events")
            .setConfigs(configs);
        AlterConfigsResourceCollection resources = new AlterConfigsResourceCollection();
        resources.add(resource);
        AlterConfigsRequestData req = new AlterConfigsRequestData().setResources(resources);

        Map<String, Object> m = ApiMessageActivation.from(req);
        Map<?, ?> cfg = (Map<?, ?>) ((List<?>) ((Map<?, ?>) ((List<?>) m.get("resources")).get(0)).get("configs")).get(0);
        assertNull(cfg.get("value"),
            "listener-prefixed ssl.truststore.certificates must redact via the endsWith match");
    }

    @Test
    public void sensitiveValueRedactionIsCaseInsensitive() {
        // The pattern matches lowercased input. A protocol that arrived with
        // unusual casing (`SSL.Keystore.Password`) MUST still trigger
        // redaction — anything else would be a trivial bypass via casing.
        AlterableConfigCollection configs = new AlterableConfigCollection();
        configs.add(new AlterableConfig().setName("SSL.Keystore.Password").setValue("Pa55w0rd"));
        AlterConfigsResource resource = new AlterConfigsResource()
            .setResourceType((byte) 2)
            .setResourceName("audit-events")
            .setConfigs(configs);
        AlterConfigsResourceCollection resources = new AlterConfigsResourceCollection();
        resources.add(resource);
        AlterConfigsRequestData req = new AlterConfigsRequestData().setResources(resources);

        Map<String, Object> m = ApiMessageActivation.from(req);
        Map<?, ?> cfg = (Map<?, ?>) ((List<?>) ((Map<?, ?>) ((List<?>) m.get("resources")).get(0)).get("configs")).get(0);
        assertNull(cfg.get("value"),
            "case-shifted variants of credential names must still redact (no trivial casing bypass)");
    }

    @Test
    public void produceRequestRecordsAreOpaqueToTheActivationWalker() {
        // Round-3 DoS adversary P0. The activation walker must NOT decompress
        // a PRODUCE request's record payload to feed it into CEL rules:
        //
        //   1. Decompression on the request path is unbounded cost — a single
        //      max.message.bytes payload can decompress to many MiB of inner
        //      records and force the walker through every one of them.
        //   2. Record values carry application-level secrets (tokens, PII,
        //      credentials). Surfacing them via the activation map would let
        //      any operator with rule-write access exfiltrate payload bytes
        //      with a rule of the form `request...records....value.startsWith(...)`.
        //
        // Pin both: the records field must come back as a tiny descriptor
        // containing sizeInBytes only — no batches, no records, no value bytes.
        // sizeInBytes is harmless and lets a rule deny on request-size patterns
        // without decompressing anything.
        SimpleRecord secret = new SimpleRecord("k".getBytes(), "TOPSECRET".getBytes());
        MemoryRecords payload = MemoryRecords.withRecords(Compression.NONE, secret);

        PartitionProduceData partition = new PartitionProduceData()
            .setIndex(0)
            .setRecords(payload);
        TopicProduceData topic = new TopicProduceData()
            .setName("audit-events")
            .setPartitionData(java.util.Collections.singletonList(partition));
        TopicProduceDataCollection topics = new TopicProduceDataCollection();
        topics.add(topic);
        ProduceRequestData req = new ProduceRequestData().setTopicData(topics);

        Map<String, Object> m = ApiMessageActivation.from(req);
        @SuppressWarnings("unchecked")
        List<Object> topicData = (List<Object>) m.get("topicData");
        Map<?, ?> firstTopic = (Map<?, ?>) topicData.get(0);
        @SuppressWarnings("unchecked")
        List<Object> partitionData = (List<Object>) firstTopic.get("partitionData");
        Map<?, ?> firstPartition = (Map<?, ?>) partitionData.get(0);

        Object recordsField = firstPartition.get("records");
        assertNotNull(recordsField, "records key must be present so size-based rules can target it");
        assertTrue(recordsField instanceof Map,
            "records value must be an opaque descriptor map, got: " + recordsField.getClass());
        Map<?, ?> descriptor = (Map<?, ?>) recordsField;
        assertEquals(1, descriptor.size(),
            "descriptor must expose sizeInBytes only — got: " + descriptor);
        assertTrue(descriptor.get("sizeInBytes") instanceof Long,
            "sizeInBytes must be CEL-friendly Long, got: " + descriptor.get("sizeInBytes"));
        assertEquals((long) payload.sizeInBytes(), descriptor.get("sizeInBytes"));

        // Defense in depth: the secret bytes must not appear ANYWHERE in the
        // string form of the activation. If a future refactor accidentally
        // re-walked the records, this check would catch it even if the per-
        // field assertions above were rewritten incorrectly.
        String full = m.toString();
        assertFalse(full.contains("TOPSECRET"),
            "record payload bytes must never reach the activation map: " + full);
        assertFalse(full.contains("batches"),
            "batches accessor must not be invoked on a records field: " + full);
    }

    @Test
    public void recursivelyHandlesAllGeneratedKafkaApiTypes() {
        // Smoke-coverage check: instantiate every public, no-arg, concrete
        // generated *RequestData under common.message and confirm extraction
        // returns *something* without throwing. The generic extractor must
        // not have any hidden coupling to a specific request type.
        String pkg = "org.apache.kafka.common.message";
        java.io.File pkgDir = locatePackageDir(pkg);
        if (pkgDir == null) {
            fail("could not locate generated message package on disk: " + pkg);
        }
        int probed = 0;
        for (java.io.File f : pkgDir.listFiles((d, n) -> n.endsWith("RequestData.java"))) {
            String className = pkg + "." + f.getName().replace(".java", "");
            try {
                Class<?> klass = Class.forName(className);
                if (!org.apache.kafka.common.protocol.ApiMessage.class.isAssignableFrom(klass)) {
                    continue;
                }
                Object instance = klass.getDeclaredConstructor().newInstance();
                Map<String, Object> m = ApiMessageActivation.from((org.apache.kafka.common.protocol.ApiMessage) instance);
                assertNotNull(m, className);
                probed++;
            } catch (NoSuchMethodException ignored) {
                // no zero-arg ctor — skip; not all generated types have one
            } catch (Exception e) {
                fail("extraction blew up on " + className + ": " + e);
            }
        }
        assertTrue(probed > 10, "expected to probe many request types, got " + probed);
    }

    @Test
    public void accessorThrowingErrorPropagatesUnwrapped() {
        // Round-17 HIGH: Method.invoke wraps any Throwable thrown by the
        // invoked method inside InvocationTargetException. Before the fix,
        // Accessor.invoke caught ReflectiveOperationException (the
        // supertype) and re-wrapped as IllegalStateException — a
        // RuntimeException ⇒ Exception, which is then caught by the
        // RuleEngine's narrowed Round-15 Walker HIGH H2 `catch (Exception)`
        // and fail-OPENs. That defeated H2's entire intent: Errors from the
        // walker should propagate to KafkaApis's outer Throwable catch and
        // surface as a visible 5xx, NOT silently fail-OPEN. This test pins
        // the unwrap-and-rethrow-cause behaviour by class.
        ExplodingErrorNode root = new ExplodingErrorNode();
        Throwable thrown = null;
        try {
            ApiMessageActivation.from(root);
        } catch (Throwable t) {
            thrown = t;
        }
        assertNotNull(thrown, "Error from accessor must propagate, not be swallowed");
        assertTrue(thrown instanceof NoClassDefFoundError,
            "expected NoClassDefFoundError to escape unwrapped; got " + thrown.getClass().getName()
                + ": " + thrown.getMessage());
    }

    @Test
    public void accessorThrowingRuntimeExceptionPropagatesUnwrapped() {
        // Sibling pin for the non-Error branch of the unwrap fix:
        // RuntimeException thrown by an accessor must escape as-is (still
        // fail-OPEN under the Exception catch — same overall posture as
        // before — but no extra IllegalStateException wrapping layer).
        ExplodingRuntimeNode root = new ExplodingRuntimeNode();
        IllegalArgumentException thrown = assertThrows(
            IllegalArgumentException.class,
            () -> ApiMessageActivation.from(root));
        assertTrue(thrown.getMessage().contains("synthetic IAE from accessor"),
            "expected unwrapped IllegalArgumentException with original message; got: "
                + thrown.getMessage());
        // Defence in depth: confirm we didn't wrap into IllegalStateException
        // (Round-17 fix replaces the old re-wrap path).
        assertEquals(IllegalArgumentException.class, thrown.getClass(),
            "RuntimeException must escape as its own class, not wrapped");
    }

    /** Fixture: accessor throws an Error to verify the unwrap path. */
    @SuppressWarnings("unused")
    public static final class ExplodingErrorNode implements org.apache.kafka.common.protocol.ApiMessage {
        public String poisoned() {
            throw new NoClassDefFoundError("synthetic NCDFE from accessor");
        }
        @Override public short apiKey() {
            return -1;
        }
        @Override public short lowestSupportedVersion() {
            return 0;
        }
        @Override public short highestSupportedVersion() {
            return 0;
        }
        @Override public org.apache.kafka.common.protocol.Message duplicate() {
            return new ExplodingErrorNode();
        }
        @Override public java.util.List<org.apache.kafka.common.protocol.types.RawTaggedField> unknownTaggedFields() {
            return java.util.Collections.emptyList();
        }
        @Override public void read(org.apache.kafka.common.protocol.Readable readable, short version) {
        }
        @Override public void write(org.apache.kafka.common.protocol.Writable writable,
                                    org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                    short version) {
        }
        @Override public int size(org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                  short version) {
            return 0;
        }
        @Override public void addSize(org.apache.kafka.common.protocol.MessageSizeAccumulator size,
                                      org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                      short version) {
        }
    }

    /** Fixture: accessor throws a RuntimeException to verify same-class propagation. */
    @SuppressWarnings("unused")
    public static final class ExplodingRuntimeNode implements org.apache.kafka.common.protocol.ApiMessage {
        public String poisoned() {
            throw new IllegalArgumentException("synthetic IAE from accessor");
        }
        @Override public short apiKey() {
            return -1;
        }
        @Override public short lowestSupportedVersion() {
            return 0;
        }
        @Override public short highestSupportedVersion() {
            return 0;
        }
        @Override public org.apache.kafka.common.protocol.Message duplicate() {
            return new ExplodingRuntimeNode();
        }
        @Override public java.util.List<org.apache.kafka.common.protocol.types.RawTaggedField> unknownTaggedFields() {
            return java.util.Collections.emptyList();
        }
        @Override public void read(org.apache.kafka.common.protocol.Readable readable, short version) {
        }
        @Override public void write(org.apache.kafka.common.protocol.Writable writable,
                                    org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                    short version) {
        }
        @Override public int size(org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                  short version) {
            return 0;
        }
        @Override public void addSize(org.apache.kafka.common.protocol.MessageSizeAccumulator size,
                                      org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                      short version) {
        }
    }

    @Test
    public void mapValuedFieldsSurfaceAsNullNotDescendedThrough() {
        // Round-18 MED E-1 (defense-in-depth): no Kafka ApiMessage shape today
        // carries a java.util.Map field — but if a future KIP adds one, the
        // pre-fix walker would have fallen through to accessorsFor(HashMap.class)
        // and descended into JDK collection internals via keySet()/values()/
        // entrySet()/clone(). The carve-out at convert() surfaces any Map as
        // null — same posture as Double/Float — so the leak vector cannot
        // appear from a schema change alone; the walker must be explicitly
        // amended to add a convertMap helper if Map-field access is desired.
        Map<String, Object> activation = ApiMessageActivation.from(new MapFieldNode());
        // The key is present (the field accessor was discovered) but the
        // value is null (the Map carve-out replaced it).
        assertTrue(activation.containsKey("labels"),
            "Map-typed field accessor must still appear in the activation; got " + activation);
        assertNull(activation.get("labels"),
            "Map-typed field VALUE must surface as null, not a walked sub-tree of " +
                "HashMap internals (keySet/values/entrySet/clone)");
        // Defence-in-depth: confirm the walker did NOT silently strip the key
        // entirely. A future regression that returns Collections.emptyMap()
        // for a Map field would lose the field's presence signal — operators
        // would see no key at all, and rules referencing the field would
        // behave the same as if the field didn't exist on the message shape.
        // We want the key visible (so missing-field vs. opaque-value is
        // distinguishable in audit) and the value null.
        assertEquals(1L, activation.get("idx"),
            "scalar siblings of a Map field must still surface normally");
    }

    /**
     * Fixture for Round-18 MED E-1: an ApiMessage whose accessor returns a
     * {@link java.util.Map} field. No real Kafka schema has this shape today,
     * but the walker's behaviour must be pinned regardless so a future
     * schema addition cannot regress to descending into JDK collection
     * internals.
     */
    @SuppressWarnings("unused")
    public static final class MapFieldNode implements org.apache.kafka.common.protocol.ApiMessage {
        public java.util.Map<String, String> labels() {
            java.util.Map<String, String> m = new java.util.LinkedHashMap<>();
            m.put("alpha", "one");
            m.put("beta", "two");
            return m;
        }
        public long idx() {
            return 1L;
        }
        @Override public short apiKey() {
            return -1;
        }
        @Override public short lowestSupportedVersion() {
            return 0;
        }
        @Override public short highestSupportedVersion() {
            return 0;
        }
        @Override public org.apache.kafka.common.protocol.Message duplicate() {
            return new MapFieldNode();
        }
        @Override public java.util.List<org.apache.kafka.common.protocol.types.RawTaggedField> unknownTaggedFields() {
            return java.util.Collections.emptyList();
        }
        @Override public void read(org.apache.kafka.common.protocol.Readable readable, short version) {
        }
        @Override public void write(org.apache.kafka.common.protocol.Writable writable,
                                    org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                    short version) {
        }
        @Override public int size(org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                  short version) {
            return 0;
        }
        @Override public void addSize(org.apache.kafka.common.protocol.MessageSizeAccumulator size,
                                      org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                      short version) {
        }
    }

    @Test
    public void mapValuedFieldsAreNullEvenWhenNestedInsideRecordOrList() {
        // Round-19 MED A-3: the existing mapValuedFieldsSurfaceAsNullNotDescendedThrough
        // test only covers a TOP-LEVEL Map field. The walker also descends into
        // nested records and list elements, so a Map nested inside a record or
        // inside a list element must surface as null at any depth — otherwise
        // the carve-out is one-deep and a future schema with a list of
        // map-bearing records would still leak via the inner-element path.
        //
        // Two coverage shapes:
        //   1. record-of-map: `outer.inner.labels` — inner record has a Map field
        //   2. list-of-record-of-map: `outer.entries[i].labels` — list elements
        //      each have a Map field

        // ---- record-of-map ----
        Map<String, Object> a1 = ApiMessageActivation.from(new RecordContainingMapNode());
        @SuppressWarnings("unchecked")
        Map<String, Object> inner = (Map<String, Object>) a1.get("inner");
        assertNotNull(inner,
            "record-typed sibling must walk into a sub-map (not be replaced wholesale); got: " + a1);
        assertTrue(inner.containsKey("labels"),
            "nested record's Map-typed field accessor must still surface as a key; got inner: " + inner);
        assertNull(inner.get("labels"),
            "Map nested inside a record MUST also surface as null (carve-out is depth-agnostic); got: " + inner);

        // ---- list-of-record-of-map ----
        Map<String, Object> a2 = ApiMessageActivation.from(new ListOfMapBearingRecordsNode());
        @SuppressWarnings("unchecked")
        java.util.List<Object> entries = (java.util.List<Object>) a2.get("entries");
        assertNotNull(entries,
            "list-typed accessor must walk into a list; got: " + a2);
        assertEquals(2, entries.size(),
            "list element count must be preserved; got: " + entries);
        for (int i = 0; i < entries.size(); i++) {
            @SuppressWarnings("unchecked")
            Map<String, Object> el = (Map<String, Object>) entries.get(i);
            assertTrue(el.containsKey("labels"),
                "list element " + i + ": Map-typed field key must still surface; got: " + el);
            assertNull(el.get("labels"),
                "list element " + i + ": Map nested inside a list element MUST surface as null; got: " + el);
        }
    }

    @Test
    public void mapImplementingIterableStillSurfacesAsNull() {
        // Round-19 MED A-4 ordering: the convert() method now checks
        // `instanceof Map` BEFORE `instanceof Iterable`. The standard JDK Map
        // types ({@code HashMap}, {@code LinkedHashMap}, {@code TreeMap})
        // do not implement Iterable, so the ordering is moot for them. But a
        // third-party Map (or a hypothetical future schema choice) that
        // implements both would, under the previous ordering, walk via
        // convertIterable — bypassing the Map carve-out entirely and
        // exposing the operator to whatever the iterator() method yields
        // (typically the entrySet's iterator, leaking key+value pairs).
        //
        // Pin the ordering directly: a fixture whose field is a Map that
        // ALSO implements Iterable must still surface as null. If the
        // ordering ever regresses, this test fails immediately.
        Map<String, Object> activation = ApiMessageActivation.from(new MapAlsoIterableFieldNode());
        assertTrue(activation.containsKey("blob"),
            "field accessor must still surface as a key; got: " + activation);
        assertNull(activation.get("blob"),
            "Map-that-also-implements-Iterable MUST surface as null (Map check must "
                + "win over the Iterable check); got: " + activation);
    }

    /**
     * Fixture for Round-19 MED A-3: an outer ApiMessage whose accessor returns
     * a nested record, and the nested record has a Map-typed field. Pins that
     * the carve-out applies at depth, not just to top-level Map fields.
     */
    @SuppressWarnings("unused")
    public static final class RecordContainingMapNode implements org.apache.kafka.common.protocol.ApiMessage {
        public InnerWithMap inner() {
            return new InnerWithMap();
        }
        @Override public short apiKey() { return -1; }
        @Override public short lowestSupportedVersion() { return 0; }
        @Override public short highestSupportedVersion() { return 0; }
        @Override public org.apache.kafka.common.protocol.Message duplicate() { return new RecordContainingMapNode(); }
        @Override public java.util.List<org.apache.kafka.common.protocol.types.RawTaggedField> unknownTaggedFields() {
            return java.util.Collections.emptyList();
        }
        @Override public void read(org.apache.kafka.common.protocol.Readable readable, short version) { }
        @Override public void write(org.apache.kafka.common.protocol.Writable writable,
                                    org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                    short version) { }
        @Override public int size(org.apache.kafka.common.protocol.ObjectSerializationCache cache, short version) { return 0; }
        @Override public void addSize(org.apache.kafka.common.protocol.MessageSizeAccumulator size,
                                      org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                      short version) { }
    }

    @SuppressWarnings("unused")
    public static final class InnerWithMap implements org.apache.kafka.common.protocol.Message {
        public java.util.Map<String, String> labels() {
            java.util.Map<String, String> m = new java.util.LinkedHashMap<>();
            m.put("k1", "v1");
            return m;
        }
        public long idx() { return 7L; }
        @Override public short lowestSupportedVersion() { return 0; }
        @Override public short highestSupportedVersion() { return 0; }
        @Override public org.apache.kafka.common.protocol.Message duplicate() { return new InnerWithMap(); }
        @Override public java.util.List<org.apache.kafka.common.protocol.types.RawTaggedField> unknownTaggedFields() {
            return java.util.Collections.emptyList();
        }
        @Override public void read(org.apache.kafka.common.protocol.Readable readable, short version) { }
        @Override public void write(org.apache.kafka.common.protocol.Writable writable,
                                    org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                    short version) { }
        @Override public int size(org.apache.kafka.common.protocol.ObjectSerializationCache cache, short version) { return 0; }
        @Override public void addSize(org.apache.kafka.common.protocol.MessageSizeAccumulator size,
                                      org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                      short version) { }
    }

    /**
     * Fixture for Round-19 MED A-3: an outer ApiMessage whose accessor returns
     * a List of records, each carrying a Map-typed field. Pins that walking
     * through a list does not silently descend into the per-element Map.
     */
    @SuppressWarnings("unused")
    public static final class ListOfMapBearingRecordsNode implements org.apache.kafka.common.protocol.ApiMessage {
        public java.util.List<InnerWithMap> entries() {
            return java.util.Arrays.asList(new InnerWithMap(), new InnerWithMap());
        }
        @Override public short apiKey() { return -1; }
        @Override public short lowestSupportedVersion() { return 0; }
        @Override public short highestSupportedVersion() { return 0; }
        @Override public org.apache.kafka.common.protocol.Message duplicate() { return new ListOfMapBearingRecordsNode(); }
        @Override public java.util.List<org.apache.kafka.common.protocol.types.RawTaggedField> unknownTaggedFields() {
            return java.util.Collections.emptyList();
        }
        @Override public void read(org.apache.kafka.common.protocol.Readable readable, short version) { }
        @Override public void write(org.apache.kafka.common.protocol.Writable writable,
                                    org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                    short version) { }
        @Override public int size(org.apache.kafka.common.protocol.ObjectSerializationCache cache, short version) { return 0; }
        @Override public void addSize(org.apache.kafka.common.protocol.MessageSizeAccumulator size,
                                      org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                      short version) { }
    }

    /**
     * Fixture for Round-19 MED A-4: an ApiMessage whose accessor returns a
     * value that implements BOTH {@link java.util.Map} and
     * {@link java.lang.Iterable}. With the wrong ordering (Iterable first),
     * convertIterable would walk via the iterator() method and leak Map
     * internals. With the correct ordering (Map first), the value surfaces
     * as null exactly like any other Map-shaped field.
     */
    @SuppressWarnings("unused")
    public static final class MapAlsoIterableFieldNode implements org.apache.kafka.common.protocol.ApiMessage {
        public MapAlsoIterable blob() {
            return new MapAlsoIterable();
        }
        @Override public short apiKey() { return -1; }
        @Override public short lowestSupportedVersion() { return 0; }
        @Override public short highestSupportedVersion() { return 0; }
        @Override public org.apache.kafka.common.protocol.Message duplicate() { return new MapAlsoIterableFieldNode(); }
        @Override public java.util.List<org.apache.kafka.common.protocol.types.RawTaggedField> unknownTaggedFields() {
            return java.util.Collections.emptyList();
        }
        @Override public void read(org.apache.kafka.common.protocol.Readable readable, short version) { }
        @Override public void write(org.apache.kafka.common.protocol.Writable writable,
                                    org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                    short version) { }
        @Override public int size(org.apache.kafka.common.protocol.ObjectSerializationCache cache, short version) { return 0; }
        @Override public void addSize(org.apache.kafka.common.protocol.MessageSizeAccumulator size,
                                      org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                      short version) { }
    }

    /**
     * Pathological hybrid: a class that is both a Map<K,V> and an
     * Iterable<V> over its values. Its iterator() yields concrete sensitive
     * strings so that if the Map check is bypassed, the leak is observable.
     */
    @SuppressWarnings({"NullableProblems", "unused"})
    public static final class MapAlsoIterable extends java.util.LinkedHashMap<String, String>
            implements Iterable<String> {
        private static final long serialVersionUID = 1L;
        public MapAlsoIterable() {
            put("secret-key", "TOKEN-DO-NOT-LEAK");
            put("audit-key", "OPERATOR-DO-NOT-LEAK");
        }
        @Override public java.util.Iterator<String> iterator() {
            return values().iterator();
        }
    }

    @Test
    public void exoticNumberSubclassesSurfaceAsNull() {
        // Round-20 MED C-2: schema-generated messages today only emit Long /
        // Integer / Short / Byte / Double / Float, so a non-Long-family Number
        // never reaches convertScalar from a real ApiMessage. But the
        // activation walker also runs on hand-rolled fixtures and (in the
        // future) on schema revisions that add atypical numeric accessors —
        // an in-process caller could expose BigDecimal, BigInteger,
        // AtomicInteger, AtomicLong, DoubleAdder, or LongAccumulator. Two
        // hazards: (1) CelNode coerces both sides of `==` via longValue(),
        // truncating BigDecimal("1.5") to 1 and silently overflowing
        // BigInteger > 2^63, so a rule `field == 100` would silently
        // over-match BigDecimal("100.5"); (2) Atomic* / Adder* /
        // Accumulator* are mutable, so the activation map would surface a
        // live reference whose value can change between sub-expressions,
        // defeating the snapshot contract. The fix in convertScalar
        // mirrors the Double/Float posture: the key stays present (rule
        // cannot probe absence via `c.field == null` from the schema
        // shape) but the value is opaque-null.
        Map<String, Object> activation = ApiMessageActivation.from(new ExoticNumberNode());
        assertTrue(activation.containsKey("bigDecimalField"),
            "BigDecimal accessor must surface its key (non-leakage contract): " + activation);
        assertNull(activation.get("bigDecimalField"),
            "BigDecimal value MUST be null to prevent silent truncation in CEL equality: " + activation);
        assertTrue(activation.containsKey("bigIntegerField"),
            "BigInteger accessor must surface its key: " + activation);
        assertNull(activation.get("bigIntegerField"),
            "BigInteger value MUST be null to prevent silent overflow via longValue(): " + activation);
        assertTrue(activation.containsKey("atomicIntegerField"),
            "AtomicInteger accessor must surface its key: " + activation);
        assertNull(activation.get("atomicIntegerField"),
            "AtomicInteger value MUST be null to prevent live-mutation oracle: " + activation);
        assertTrue(activation.containsKey("atomicLongField"),
            "AtomicLong accessor must surface its key: " + activation);
        assertNull(activation.get("atomicLongField"),
            "AtomicLong value MUST be null to prevent live-mutation oracle: " + activation);
    }

    /**
     * Fixture for Round-20 MED C-2: an ApiMessage exposing Number subclasses
     * outside the schema-supported {Long, Integer, Short, Byte, Double,
     * Float} set. Each accessor returns a fresh instance with a non-default
     * value so a regression that surfaces the live object (instead of null)
     * would be observable.
     */
    @SuppressWarnings("unused")
    public static final class ExoticNumberNode implements org.apache.kafka.common.protocol.ApiMessage {
        public java.math.BigDecimal bigDecimalField() {
            return new java.math.BigDecimal("100.5");
        }
        public java.math.BigInteger bigIntegerField() {
            return new java.math.BigInteger("99999999999999999999");
        }
        public java.util.concurrent.atomic.AtomicInteger atomicIntegerField() {
            return new java.util.concurrent.atomic.AtomicInteger(7);
        }
        public java.util.concurrent.atomic.AtomicLong atomicLongField() {
            return new java.util.concurrent.atomic.AtomicLong(11L);
        }
        @Override public short apiKey() { return -1; }
        @Override public short lowestSupportedVersion() { return 0; }
        @Override public short highestSupportedVersion() { return 0; }
        @Override public org.apache.kafka.common.protocol.Message duplicate() { return new ExoticNumberNode(); }
        @Override public java.util.List<org.apache.kafka.common.protocol.types.RawTaggedField> unknownTaggedFields() {
            return java.util.Collections.emptyList();
        }
        @Override public void read(org.apache.kafka.common.protocol.Readable readable, short version) { }
        @Override public void write(org.apache.kafka.common.protocol.Writable writable,
                                    org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                    short version) { }
        @Override public int size(org.apache.kafka.common.protocol.ObjectSerializationCache cache, short version) { return 0; }
        @Override public void addSize(org.apache.kafka.common.protocol.MessageSizeAccumulator size,
                                      org.apache.kafka.common.protocol.ObjectSerializationCache cache,
                                      short version) { }
    }

    private static java.io.File locatePackageDir(String pkg) {
        String rel = pkg.replace('.', '/');
        for (String root : new String[]{
            "clients/build/generated/main/java/",
            "../clients/build/generated/main/java/",
            "../../clients/build/generated/main/java/"
        }) {
            java.io.File f = new java.io.File(root + rel);
            if (f.isDirectory()) {
                return f;
            }
        }
        return null;
    }
}
