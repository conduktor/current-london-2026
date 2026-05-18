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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RuleDecisionTest {

    @Test
    public void allowIsSingleton() {
        assertSame(RuleDecision.ALLOW, RuleDecision.allow());
        assertFalse(RuleDecision.ALLOW.denied());
        assertNull(RuleDecision.ALLOW.denyingRuleId());
    }

    @Test
    public void denyCarriesErrorCodeAndRuleId() {
        RuleDecision d = RuleDecision.deny(42, "rule-1");
        assertTrue(d.denied());
        assertEquals(42, d.errorCode());
        assertEquals("rule-1", d.denyingRuleId());
    }

    @Test
    public void allowedDecisionHasNoErrorCode() {
        assertEquals(0, RuleDecision.ALLOW.errorCode());
        assertNotNull(RuleDecision.ALLOW.toString());
    }
}
