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
 * R33 #292 [HIGH]: ConfigDef-level admission test for
 * {@code governance.bypass.principals}.
 *
 * <p>Without this validator the ConfigDef declares the bypass list as a
 * bare {@code STRING}, which silently accepts any operator-supplied value
 * at admin-API write time. A malformed value (missing {@code :} separator,
 * blank type or name, comma-confusable separator typo, invisible-glyph
 * smuggle, etc.) is then persisted to KRaft metadata and survives until
 * the next broker restart — at which point {@code BrokerGovernanceBootstrap}
 * calls {@link RuleEngine#parseBypassPrincipals(String)} and the broker
 * refuses to start. The admin who typed the bad value months earlier sees
 * a successful ack and never associates the eventual restart failure with
 * their change.
 *
 * <p>Running the same parser at admin-API admission time closes that
 * delayed time-bomb: the malformed value is rejected loudly, on the wire,
 * before it ever lands in the metadata log. The validator delegates to
 * {@link RuleEngine#parseBypassPrincipals(String)} so the admission rule
 * is exactly the startup rule — no second source of truth for what counts
 * as a valid bypass principal. Any future hardening of the parser
 * (R30/R31/R32/R33 codepoint/confusable closures) is picked up by the
 * validator automatically.
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
