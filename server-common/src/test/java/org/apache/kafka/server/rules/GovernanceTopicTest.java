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
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GovernanceTopicTest {

    @Test
    public void topicNameIsInternal() {
        assertEquals("__governance", GovernanceTopic.NAME);
        assertTrue(GovernanceTopic.NAME.startsWith("__"),
            "internal-topic convention: leading double underscore");
    }

    @Test
    public void readerClientIdEmbedsBrokerAndIsExempt() {
        String cid = GovernanceTopic.readerClientId(7);
        assertTrue(cid.startsWith(RuleEngine.INTERNAL_CLIENT_ID_PREFIX),
            "reader client id must start with the engine's exempt prefix");
        assertTrue(cid.contains("7"),
            "client id should identify the broker for log/quota visibility");
    }

    @Test
    public void readerClientIdIsDistinctPerBroker() {
        assertEquals(false, GovernanceTopic.readerClientId(1)
            .equals(GovernanceTopic.readerClientId(2)));
    }
}
