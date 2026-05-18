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
package org.apache.kafka.network.http;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HostBindingValidatorTest {

    // ----- the canonical wildcard forms operators actually type -----

    @Test
    void ipv4DottedZeroIsWildcard() {
        assertTrue(HostBindingValidator.isWildcardBind("0.0.0.0"));
    }

    @Test
    void ipv6DoubleColonIsWildcard() {
        // The compressed all-zeros IPv6 form. Same operational exposure as IPv4 0.0.0.0.
        assertTrue(HostBindingValidator.isWildcardBind("::"));
    }

    @Test
    void ipv6FullyExpandedZeroIsWildcard() {
        assertTrue(HostBindingValidator.isWildcardBind("0:0:0:0:0:0:0:0"));
    }

    @Test
    void bracketedIpv6WildcardIsWildcard() {
        // Operators copy-pasting from a URL strip the brackets — but the validator must accept the bracketed form
        // too, because some configs are literally taken from a `host:port` triple.
        assertTrue(HostBindingValidator.isWildcardBind("[::]"));
    }

    // ----- blank/null are treated as wildcard because Jetty does -----

    @Test
    void nullIsWildcard() {
        assertTrue(HostBindingValidator.isWildcardBind(null));
    }

    @Test
    void emptyStringIsWildcard() {
        // Jetty resolves an empty/blank host to "all interfaces", which is the same exposure as 0.0.0.0.
        assertTrue(HostBindingValidator.isWildcardBind(""));
    }

    @Test
    void whitespaceOnlyIsWildcard() {
        // Operators sometimes set http.bridge.host="   " in a config file and expect it to mean "unset". Treat the
        // same as Jetty would (= all interfaces).
        assertTrue(HostBindingValidator.isWildcardBind("   "));
    }

    @Test
    void leadingTrailingWhitespaceAroundZeroIsWildcard() {
        // The trim step normalises "  0.0.0.0  " before resolution. Without trimming, getAllByName would treat the
        // whitespace as part of the hostname and throw UnknownHostException — losing the wildcard signal.
        assertTrue(HostBindingValidator.isWildcardBind("  0.0.0.0  "));
    }

    // ----- the loopback and "real" addresses are NOT wildcard -----

    @Test
    void loopbackIsNotWildcard() {
        assertFalse(HostBindingValidator.isWildcardBind("127.0.0.1"));
    }

    @Test
    void ipv6LoopbackIsNotWildcard() {
        assertFalse(HostBindingValidator.isWildcardBind("::1"));
    }

    @Test
    void specificPrivateAddressIsNotWildcard() {
        // 10.0.0.1 is not a wildcard — it's a specific interface. Private-network reachability is a separate
        // concern from "is this address an any-local address?", and the validator only owns the latter question.
        assertFalse(HostBindingValidator.isWildcardBind("10.0.0.1"));
    }

    // ----- unresolvable inputs degrade safely -----

    @Test
    void unresolvableHostReturnsFalse() {
        // A bad value isn't this helper's problem: Jetty will fail loudly when it tries to bind. We must not emit
        // a wildcard WARN for "this hostname has no DNS record" — that would be a false positive every time the
        // operator typos a value.
        assertFalse(HostBindingValidator.isWildcardBind(
            "this-host-does-not-exist-and-should-never-resolve-anywhere.invalid"));
    }
}
