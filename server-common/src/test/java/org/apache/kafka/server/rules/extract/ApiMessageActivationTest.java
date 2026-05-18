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

import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData.ScramCredentialUpsertion;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfigCollection;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.server.rules.cel.CelCompiler;
import org.apache.kafka.server.rules.cel.CelProgram;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
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
        // Codex deep-audit P1 part 3: the reflection walk recurses into any
        // object with at least one accessor. Kafka's generated DTOs don't
        // contain cycles, so this isn't reachable from a well-formed protocol
        // message — but a hostile request or a future protocol with deep
        // nesting we never anticipated must not be able to blow the stack or
        // spin the walker forever. MAX_DEPTH=32 caps the walk.
        //
        // A self-referencing test class is the simplest way to demonstrate the
        // cap: without the depth bound, toMap() would never return.
        SelfReferencingNode root = new SelfReferencingNode();
        Map<String, Object> m = ApiMessageActivation.from(root);
        assertNotNull(m, "depth-bounded walk must terminate and return a non-null map");
        // Drill down to MAX_DEPTH; somewhere along the way the recursion must
        // bottom out. We verify by walking until the inner map is empty.
        int depth = 0;
        Map<?, ?> current = m;
        while (!current.isEmpty() && depth < ApiMessageActivation.MAX_DEPTH + 10) {
            Object next = current.get("child");
            assertNotNull(next, "child accessor should yield a map until depth bound is hit");
            assertTrue(next instanceof Map,
                "child must be a recursively-walked Map (until the depth bound truncates)");
            current = (Map<?, ?>) next;
            depth++;
        }
        assertTrue(current.isEmpty(),
            "recursion must bottom out at an empty map by MAX_DEPTH; reached depth=" + depth);
        assertTrue(depth <= ApiMessageActivation.MAX_DEPTH,
            "recursion must terminate at or before MAX_DEPTH=" + ApiMessageActivation.MAX_DEPTH +
                "; observed depth=" + depth);
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
