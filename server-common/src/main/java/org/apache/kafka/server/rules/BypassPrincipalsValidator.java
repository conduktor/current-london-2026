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

import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;

/**
 * R33 #292 / R34-A-1 [HIGH]: ConfigDef-level admission test for
 * {@code governance.bypass.principals}.
 *
 * <h2>What this validator actually closes (narrow)</h2>
 *
 * <p>Fires at {@code ServerConfigs.CONFIG_DEF.parse(...)} time — which is
 * invoked during {@code KafkaConfig} construction at broker startup. A
 * malformed value in {@code server.properties} therefore fails fast at
 * config-parse time with config-name attribution and LogSafe sanitisation,
 * instead of falling through to {@link RuleEngine#parseBypassPrincipals(String)}
 * later in {@code BrokerGovernanceBootstrap}. Two narrow benefits:
 * <ul>
 *   <li>Earlier, louder failure with the config name embedded in the
 *       diagnostic (operators see which config is at fault before any
 *       broker subsystem starts).</li>
 *   <li>Defense-in-depth so the static-config-parse rejection rule and the
 *       bootstrap rejection rule cannot drift — both delegate to the same
 *       {@link RuleEngine#parseBypassPrincipals(String)} parser.</li>
 * </ul>
 *
 * <h2>What this validator does NOT close (the real time-bomb)</h2>
 *
 * <p><strong>This validator is unreachable on the admin-API path.</strong>
 * The original R33 #292 commit narrative — that running the parser at
 * admin-API admission time would catch malformed values before they reach
 * KRaft — was incorrect. The actual control flow:
 *
 * <ul>
 *   <li><b>{@code kafka-configs --bootstrap-server} (broker-routed).</b>
 *       {@code DynamicBrokerConfig.validateConfigs} (
 *       {@code core/.../DynamicBrokerConfig.scala:142}) intersects the
 *       incoming property set with
 *       {@code DynamicConfig.Broker.nonDynamicProps} and throws
 *       {@code ConfigException("Cannot update these configs dynamically: ...")}
 *       <em>before</em> {@code validateConfigTypes} (line 145) ever invokes
 *       {@code DynamicConfig.Broker.validate}, which is the only path that
 *       would have triggered this validator on an admin RPC. Reason:
 *       {@code governance.bypass.principals} is not in
 *       {@code DynamicBrokerConfig.AllDynamicConfigs}.</li>
 *   <li><b>{@code kafka-configs --bootstrap-controller} (KIP-919,
 *       controller-direct).</b> Admin RPC lands at
 *       {@code ControllerConfigurationValidator.validate} (
 *       {@code core/.../ControllerConfigurationValidator.scala:122}), which
 *       for {@code BROKER} resources validates <em>only the resource name</em>
 *       (broker id). New values pass through unchecked. The bad value lands
 *       in the metadata log; the broker refuses to start at next restart
 *       (BrokerGovernanceBootstrap), by which time the typing admin has lost
 *       context. This is the real time-bomb. Tracked as task R34-B-1.</li>
 * </ul>
 *
 * <p>Closing the time-bomb properly requires either (a) making
 * {@code governance.bypass.principals} dynamic — which in turn requires
 * wiring a {@code BrokerReconfigurable} listener so the in-memory bypass
 * set hot-swaps (R34-A-2), or (b) extending
 * {@code ControllerConfigurationValidator} to value-validate BROKER configs
 * against {@code ServerConfigs.CONFIG_DEF} (R34-B-1). This validator stays
 * because both of those follow-ups also rely on it — but its current reach
 * is the static-config-parse path only.
 *
 * <h2>Empty / {@code null} acceptance rationale</h2>
 *
 * <p>Empty input is accepted here because the empty-set rejection is a
 * broker-startup contract enforced in {@link RuleEngine#RuleEngine(java.util.Set)}
 * — admins legitimately need to be able to write the empty default into a
 * ConfigDef-managed config (and the broker is not running with the new
 * value the moment the admin writes it). Conflating the two would block
 * legitimate clear-and-resubmit flows.
 *
 * <p>{@code null} values are also accepted: ConfigDef passes the parsed
 * config value (which for an empty/unset STRING config is the configured
 * default, never {@code null} in practice for this config) — the
 * defensive {@code null}-tolerance mirrors
 * {@link RuleEngine#parseBypassPrincipals(String)} itself, which returns
 * an empty set on {@code null}.
 */
public final class BypassPrincipalsValidator implements Validator {

    @Override
    public void ensureValid(String name, Object value) {
        if (value == null) {
            return;
        }
        String raw = value.toString();
        try {
            RuleEngine.parseBypassPrincipals(raw);
        } catch (IllegalArgumentException e) {
            // RuleEngine.parseBypassPrincipals already LogSafe-sanitises the
            // offending segment before embedding it in the exception
            // message (Round-20 HIGH A-1). But ConfigException's own
            // (name, value, message) formatter additionally embeds the
            // raw `value` argument verbatim into its toString as
            // "Invalid value <value> for configuration <name>: <message>".
            // A malicious config value with CR/LF would therefore land
            // unescaped in any logger that picked up ConfigException.
            // Pre-sanitise via LogSafe so the value slot is log-safe too.
            // The admin still sees their input — control codepoints appear
            // as escaped backslash-u-XXXX rather than raw bytes, mirroring
            // how the reason slot already presents them.
            throw new ConfigException(name, LogSafe.sanitize(raw), e.getMessage());
        }
    }

    @Override
    public String toString() {
        return "[semicolon-separated principals of the form type:name; "
            + "blank, malformed, comma-separated, or hazardous-codepoint "
            + "entries are rejected — empty list is accepted at config "
            + "write time but rejected at broker startup]";
    }
}
