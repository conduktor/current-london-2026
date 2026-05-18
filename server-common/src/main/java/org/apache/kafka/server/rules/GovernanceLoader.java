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

import org.apache.kafka.server.rules.json.RuleEnvelopeException;
import org.apache.kafka.server.rules.json.RuleJsonCodec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Pure-logic side of the {@code __governance} compacted-topic consumer.
 *
 * <p>This class is deliberately Kafka-API-free: it consumes {@code (key, value)}
 * pairs from any source — the broker's actual {@code KafkaConsumer} thread,
 * a test harness, a {@code MockConsumer} replay — through {@link #apply}, and
 * publishes the accumulated state atomically into the {@link RuleEngine} on
 * {@link #commit}.
 *
 * <p>Semantics:
 *
 * <ul>
 *   <li><b>Update</b> ({@code value != null}): decode the envelope into a Rule
 *       and place it in the working set. If decode rejects the envelope (bad
 *       JSON, unknown api key, uncompilable CEL, etc.), the record is logged
 *       at WARN and dropped — the previously installed version of that rule
 *       (if any) remains in the working set untouched.</li>
 *   <li><b>Tombstone</b> ({@code value == null}): remove the rule with this id
 *       from the working set. Idempotent: removing an absent id is a no-op,
 *       consistent with at-least-once delivery from the rules topic.</li>
 *   <li><b>Null key</b>: cannot identify the rule — record is logged at WARN
 *       and dropped. The loader must not crash on records that violate the
 *       key/value contract; it must keep draining the topic.</li>
 *   <li><b>Commit</b>: builds a {@link RuleSet} snapshot from the working
 *       state and installs it atomically. Every commit is observable through
 *       {@link RuleEngine#active()}.</li>
 * </ul>
 *
 * <p>The working state survives across commits. A typical reader thread will
 * apply every record in a poll batch and call {@link #commit} once at the end
 * of the batch — that gives the engine one atomic swap per batch rather than
 * one per record, which keeps the request path's view of the world stable
 * within a batch and avoids unnecessary churn.
 *
 * <p>Not thread-safe: a single reader thread is expected to own one instance.
 * Multiple loaders pointed at the same engine are fine (they are independent
 * RuleSetBuilders that race on {@link RuleEngine#install}, which is itself
 * atomic), but doing so means rules from different sources will overwrite each
 * other on commit — not a useful configuration in practice.
 */
public final class GovernanceLoader {

    private static final Logger LOG = LoggerFactory.getLogger(GovernanceLoader.class);

    private final RuleEngine engine;
    private final RuleSetBuilder working = new RuleSetBuilder();

    public GovernanceLoader(RuleEngine engine) {
        this.engine = Objects.requireNonNull(engine, "engine");
    }

    /**
     * Apply a single {@code __governance} record to the working state.
     *
     * <p><b>Validation order matters.</b> For an update record we decode the
     * envelope <em>before</em> mutating the working state. This is what makes
     * a batch of {@code [DELETE X, PUT X-malformed]} safe: the delete is the
     * second-to-last form of the record stream for key X, and a malformed
     * update record in compaction-key order would already have failed to
     * install in an earlier batch — there is no good reason to let a poison
     * update silently delete the previously-good version of the rule in the
     * working state. Equivalently: we only ever transition the working state
     * to a new, decoded, validated rule, never to a half-built one.
     *
     * <p>Tombstones still apply unconditionally — that is the documented
     * semantics for a compacted topic: a null-value record means "this key
     * is now removed". A malformed update for the same key in the same batch
     * cannot un-do a valid tombstone that ordered before it.
     *
     * @param key the record key (rule id); null records are dropped
     * @param value the record value (envelope JSON); null is a tombstone
     * @return {@code true} when the record was successfully processed into the
     *         working state (a valid update was decoded and put, or a tombstone
     *         was applied — idempotent or not). {@code false} when the record
     *         was rejected (null key, malformed envelope, or builder cap
     *         exceeded) and therefore made no contribution to the working
     *         state. Callers in {@code BrokerGovernanceBootstrap} use this
     *         signal to decide whether the drain made forward progress past
     *         a truncation-reset: a drain that read N records but applied
     *         none must NOT clear the {@code holdingStalePostTruncation}
     *         flag nor publish the still-empty working set, because doing
     *         so would replace the engine's previously-good {@code active()}
     *         with {@code RuleSet.EMPTY} on a topic that contains only
     *         records this loader cannot parse — a fail-stale-to-empty
     *         regression. Audit B1.
     */
    public boolean apply(String key, byte[] value) {
        if (key == null) {
            LOG.warn("dropping __governance record with null key (value present={})", value != null);
            return false;
        }
        // Round-12 audit (tombstone/compaction sub-agent, MEDIUM-1): reject
        // operator-published records whose key uses the engine-reserved
        // "__name__" shape on BOTH the update and tombstone paths. RuleJsonCodec
        // enforces this on update records inside decode(), but a tombstone
        // (value == null) is not routed through the codec — the previous code
        // path went straight to working.remove(key). No __name__-shape rule
        // exists in the working set today (the codec rejects them at intake),
        // so the existing behaviour was a no-op rather than a vulnerability;
        // but if a future engine-internal sentinel ever populates a __name__
        // rule into the working set, an operator-published tombstone would
        // silently delete it. Closing the asymmetry here removes that future
        // surface and keeps the rejection contract symmetric across both
        // record shapes.
        if (RuleJsonCodec.isReservedNameShape(key)) {
            LOG.warn("dropping __governance record for rule id '{}' — uses the reserved \"__name__\" shape " +
                "(double-underscore prefix and suffix); these ids are reserved for engine-internal " +
                "synthetic decisions and may not be authored by operators (record was a {})",
                key, value == null ? "tombstone" : "update");
            return false;
        }
        if (value == null) {
            // Tombstone — RuleSetBuilder.remove() is idempotent on an absent id.
            // We return true even when the id was absent: the LOG said "this
            // key is now removed" and we honoured that. From the held-stale
            // perspective, a successful tombstone is forward progress through
            // the log — we have a positive signal that the loader is making
            // sense of the records it sees.
            working.remove(key);
            return true;
        }
        // Decode-then-mutate: a malformed envelope must not silently displace
        // the previously installed good version of this rule. Only commit the
        // mutation once we hold a fully-validated Rule.
        final Rule rule;
        try {
            rule = RuleJsonCodec.decode(key, value);
        } catch (RuleEnvelopeException e) {
            LOG.warn("rejecting bad __governance envelope for rule '{}': {} — " +
                "previously installed version of this rule (if any) is preserved", key, e.getMessage());
            return false;
        }
        // RuleSetBuilder.put rejects new ids past its MAX_RULES cap. Treat
        // that like a malformed envelope at this level: log, preserve prior
        // state, keep draining the topic. Updates to an existing id never
        // trip the cap (the set does not grow).
        try {
            working.put(rule);
            return true;
        } catch (IllegalStateException e) {
            LOG.warn("rejecting __governance update for rule '{}': {} — " +
                "previously installed RuleSet is preserved", key, e.getMessage());
            return false;
        }
    }

    /**
     * Atomically install the working state as the engine's new active
     * {@link RuleSet}. Concurrent request-path readers observe either the
     * previous snapshot or this new one — never a torn intermediate.
     */
    public void commit() {
        engine.install(working.build());
    }

    /**
     * Drop all in-flight rules from the working set. Used by the broker
     * bootstrap when it detects that the underlying {@code __governance}
     * log was truncated below the cursor it had already advanced past
     * (e.g. after a leader-election with epoch divergence). Without a
     * reset, the working set retains zombie rules that no longer exist
     * on the topic and re-installs them on every {@link #commit}; with a
     * reset, the next drain re-reads the log from scratch and reaches a
     * fresh-and-correct working state.
     *
     * <p>Does not touch the engine's currently-installed {@link RuleSet}:
     * the caller is expected to follow up with the re-drain and a single
     * {@link #commit}, so the request path's view never goes through an
     * intermediate "empty" state.
     */
    public void reset() {
        working.from(RuleSet.EMPTY);
    }
}
