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
package org.apache.kafka.server.rules;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.server.rules.cel.CelCompiler;
import org.apache.kafka.server.rules.cel.CelProgram;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RuleTest {

    private static final CelProgram TRUE = CelCompiler.compile("true");

    @Test
    public void carriesAllFields() {
        Rule rule = new Rule("r1", Arrays.asList(ApiKeys.CREATE_TOPICS), RuleAction.DENY, "true", 42, TRUE);
        assertEquals("r1", rule.id());
        assertEquals(Collections.singletonList(ApiKeys.CREATE_TOPICS), rule.apiKeys());
        assertSame(RuleAction.DENY, rule.action());
        assertEquals("true", rule.whenSource());
        assertEquals(42, rule.errorCode());
        assertSame(TRUE, rule.compiled());
    }

    @Test
    public void apiKeysAreCopiedAndImmutable() {
        java.util.List<ApiKeys> input = new java.util.ArrayList<>();
        input.add(ApiKeys.CREATE_TOPICS);
        Rule rule = new Rule("r1", input, RuleAction.DENY, "true", 42, TRUE);
        input.clear();
        assertEquals(1, rule.apiKeys().size());
        assertThrows(UnsupportedOperationException.class, () -> rule.apiKeys().add(ApiKeys.FETCH));
    }

    @Test
    public void rejectsEmptyApiKeys() {
        assertThrows(IllegalArgumentException.class,
            () -> new Rule("r1", Collections.emptyList(), RuleAction.DENY, "true", 42, TRUE));
    }

    @Test
    public void rejectsNullFields() {
        assertThrows(NullPointerException.class,
            () -> new Rule(null, Collections.singletonList(ApiKeys.FETCH), RuleAction.DENY, "true", 42, TRUE));
        assertThrows(NullPointerException.class,
            () -> new Rule("r1", null, RuleAction.DENY, "true", 42, TRUE));
        assertThrows(NullPointerException.class,
            () -> new Rule("r1", Collections.singletonList(ApiKeys.FETCH), null, "true", 42, TRUE));
        assertThrows(NullPointerException.class,
            () -> new Rule("r1", Collections.singletonList(ApiKeys.FETCH), RuleAction.DENY, null, 42, TRUE));
        assertThrows(NullPointerException.class,
            () -> new Rule("r1", Collections.singletonList(ApiKeys.FETCH), RuleAction.DENY, "true", 42, null));
    }

    @Test
    public void equalsAndHashCodeByContent() {
        Rule a = new Rule("r1", Collections.singletonList(ApiKeys.FETCH), RuleAction.DENY, "true", 42, TRUE);
        Rule b = new Rule("r1", Collections.singletonList(ApiKeys.FETCH), RuleAction.DENY, "true", 42, TRUE);
        Rule c = new Rule("r2", Collections.singletonList(ApiKeys.FETCH), RuleAction.DENY, "true", 42, TRUE);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertTrue(!a.equals(c));
    }
}
