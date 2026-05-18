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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.server.views.ViewTopicConfig;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the three view-related topic configs:
 * {@link ViewTopicConfig#VIEW_BACKING_TOPIC_CONFIG},
 * {@link ViewTopicConfig#VIEW_CEL_PREDICATE_CONFIG},
 * {@link ViewTopicConfig#VIEW_OFFSET_MODE_CONFIG}.
 *
 * The contract under test:
 *  - All three default to {@code null}; a topic without any view-config is a regular topic.
 *  - Setting all three together yields a valid view config that round-trips through LogConfig.
 *  - Setting any strict non-empty subset is rejected at validate() time
 *    (InvalidConfigurationException) — "all or none".
 *  - {@code view.offset.mode} is restricted to {@code source_sparse} at the ConfigDef level
 *    (ConfigException at parse).
 *  - The CEL predicate is compile-checked at validate() time so bad predicates are rejected
 *    on topic create/alter, not on the fetch hot path.
 */
class ViewLogConfigTest {

    @Test
    void defaultsHaveNoViewConfig() {
        LogConfig cfg = new LogConfig(new Properties());
        assertFalse(cfg.isView(), "fresh LogConfig must not look like a view");
        assertNull(cfg.viewBackingTopic());
        assertNull(cfg.viewCelPredicate());
        assertNull(cfg.viewOffsetMode());
    }

    @Test
    void allThreeSetProducesAView() {
        Properties props = new Properties();
        props.put(ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG, "orders-raw");
        props.put(ViewTopicConfig.VIEW_CEL_PREDICATE_CONFIG, "body.color == 'red'");
        props.put(ViewTopicConfig.VIEW_OFFSET_MODE_CONFIG, ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE);
        LogConfig.validate(props);
        LogConfig cfg = new LogConfig(props);
        assertTrue(cfg.isView(), "configured topic must be recognized as a view");
        assertEquals("orders-raw", cfg.viewBackingTopic());
        assertEquals("body.color == 'red'", cfg.viewCelPredicate());
        assertEquals(ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE, cfg.viewOffsetMode());
    }

    @Test
    void rejectsUnknownOffsetMode() {
        // ConfigDef validates the enum at parse time, before validateValues runs.
        Properties props = viewProps();
        props.put(ViewTopicConfig.VIEW_OFFSET_MODE_CONFIG, "renumber");
        assertThrows(ConfigException.class, () -> LogConfig.validate(props));
    }

    @Test
    void rejectsBackingOnly() {
        Properties props = new Properties();
        props.put(ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG, "orders-raw");
        assertThrows(InvalidConfigurationException.class, () -> LogConfig.validate(props));
    }

    @Test
    void rejectsPredicateOnly() {
        Properties props = new Properties();
        props.put(ViewTopicConfig.VIEW_CEL_PREDICATE_CONFIG, "body.x == 1");
        assertThrows(InvalidConfigurationException.class, () -> LogConfig.validate(props));
    }

    @Test
    void rejectsOffsetModeOnly() {
        Properties props = new Properties();
        props.put(ViewTopicConfig.VIEW_OFFSET_MODE_CONFIG, ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE);
        assertThrows(InvalidConfigurationException.class, () -> LogConfig.validate(props));
    }

    @Test
    void rejectsTwoOfThree() {
        Properties props = new Properties();
        props.put(ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG, "orders-raw");
        props.put(ViewTopicConfig.VIEW_OFFSET_MODE_CONFIG, ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE);
        assertThrows(InvalidConfigurationException.class, () -> LogConfig.validate(props));
    }

    @Test
    void rejectsBlankBackingTopic() {
        Properties props = viewProps();
        props.put(ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG, "  ");
        assertThrows(InvalidConfigurationException.class, () -> LogConfig.validate(props));
    }

    @Test
    void rejectsEmptyBackingTopic() {
        // LogConfig.validate has no topic-name context, so the self-loop rule is enforced by
        // ControllerConfigurationValidator. Here we only verify the local non-empty constraint.
        Properties props = viewProps();
        props.put(ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG, "");
        assertThrows(InvalidConfigurationException.class, () -> LogConfig.validate(props));
    }

    @Test
    void rejectsPredicateThatFailsCompilation() {
        // The sandboxed compiler rejects function calls; reusing that here means a bad
        // predicate is caught at config-set time, never on the fetch path.
        Properties props = viewProps();
        props.put(ViewTopicConfig.VIEW_CEL_PREDICATE_CONFIG, "body.s.matches('.*foo.*')");
        InvalidConfigurationException ex = assertThrows(InvalidConfigurationException.class,
                () -> LogConfig.validate(props));
        assertTrue(ex.getMessage().toLowerCase(java.util.Locale.ROOT)
                .contains(ViewTopicConfig.VIEW_CEL_PREDICATE_CONFIG),
                () -> "exception should name the offending config, got: " + ex.getMessage());
    }

    @Test
    void rejectsPredicateThatIsSyntaxError() {
        Properties props = viewProps();
        props.put(ViewTopicConfig.VIEW_CEL_PREDICATE_CONFIG, "body.color ==");
        assertThrows(InvalidConfigurationException.class, () -> LogConfig.validate(props));
    }

    @Test
    void rejectsBlankPredicate() {
        Properties props = viewProps();
        props.put(ViewTopicConfig.VIEW_CEL_PREDICATE_CONFIG, "   ");
        assertThrows(InvalidConfigurationException.class, () -> LogConfig.validate(props));
    }

    @Test
    void configNameIsRegistered() {
        // Make sure topic config admin paths see our keys when listing valid configs.
        assertTrue(LogConfig.configNames().contains(ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG));
        assertTrue(LogConfig.configNames().contains(ViewTopicConfig.VIEW_CEL_PREDICATE_CONFIG));
        assertTrue(LogConfig.configNames().contains(ViewTopicConfig.VIEW_OFFSET_MODE_CONFIG));
    }

    private static Properties viewProps() {
        Properties p = new Properties();
        p.put(ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG, "orders-raw");
        p.put(ViewTopicConfig.VIEW_CEL_PREDICATE_CONFIG, "body.color == 'red'");
        p.put(ViewTopicConfig.VIEW_OFFSET_MODE_CONFIG, ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE);
        return p;
    }
}
