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
package org.apache.kafka.server.views;

/**
 * Per-topic configuration keys that turn a regular topic into a read-only view.
 *
 * A topic is a view iff all three of {@link #VIEW_BACKING_TOPIC_CONFIG},
 * {@link #VIEW_CEL_PREDICATE_CONFIG} and {@link #VIEW_OFFSET_MODE_CONFIG} are set. Setting any
 * subset of them is a configuration error — {@link
 * org.apache.kafka.storage.internals.log.LogConfig#validateValues(java.util.Map)} rejects
 * partial views at create/alter time so brokers never observe an inconsistent state.
 *
 * The constants live in {@code server-common} rather than in
 * {@code org.apache.kafka.common.config.TopicConfig} because the topic-views feature is broker-only;
 * the client jar must remain untouched (see PROMPT.md).
 *
 * <h3>Authorization model</h3>
 *
 * A view is its own topic for authorization purposes. The broker checks {@code READ TOPIC} on the
 * <em>view's</em> topic name when a consumer issues a fetch — the redirect to the backing topic
 * happens after the ACL check, inside the same broker, and the backing topic's ACLs are
 * <em>not</em> consulted on the redirect path. This is deliberate: views exist precisely to
 * expose a filtered, narrower projection of a sensitive backing topic to clients that should
 * not be able to read the unfiltered stream.
 *
 * <p>The operational implication: granting a principal {@code READ} on a view grants them
 * access to all records the predicate returns from the backing topic, even when that principal
 * does not have {@code READ} on the backing topic itself. Operators must therefore treat the
 * choice of {@code view.cel.predicate} as a security-relevant decision — a permissive predicate
 * is effectively an authorization grant on the matching subset of the backing topic. Operators
 * who want to restrict access to the backing topic from view consumers should set ACLs on the
 * backing topic accordingly (e.g. deny {@code READ} for the view consumer's principals on the
 * backing topic); those ACLs still apply when the consumer fetches the backing topic directly.
 *
 * <p>Produce-to-view is unconditionally rejected with {@code INVALID_REQUEST} before any ACL
 * check, before backing-topic resolution, and before record validation. Views are read-only by
 * construction; allowing a write to bypass the predicate would violate the contract that a
 * view's content is fully determined by the backing topic and the predicate.
 *
 * <p>Self-loop ({@code view.backing.topic} equals the configured topic) is rejected at
 * {@code CreateTopics} / {@code AlterConfigs} time by the controller's configuration validator
 * so it never reaches a live broker.
 */
public final class ViewTopicConfig {

    private ViewTopicConfig() {
    }

    /** Physical topic whose records back this view. Empty / null means "not a view". */
    public static final String VIEW_BACKING_TOPIC_CONFIG = "view.backing.topic";

    /** CEL-style predicate filtering records from the backing topic. */
    public static final String VIEW_CEL_PREDICATE_CONFIG = "view.cel.predicate";

    /** How offsets from the backing topic are surfaced through the view. */
    public static final String VIEW_OFFSET_MODE_CONFIG = "view.offset.mode";

    /** Only supported value: records keep the offsets they had in the backing topic (sparse). */
    public static final String VIEW_OFFSET_MODE_SOURCE_SPARSE = "source_sparse";

    public static final String VIEW_BACKING_TOPIC_DOC =
            "Name of the physical topic backing this read-only view. When set together with "
                    + VIEW_CEL_PREDICATE_CONFIG + " and " + VIEW_OFFSET_MODE_CONFIG
                    + ", reads from this topic transparently fetch records from the backing "
                    + "topic, filter them through the predicate, and surface only matching "
                    + "records at their source offsets. Producing to a view is rejected.";

    public static final String VIEW_CEL_PREDICATE_DOC =
            "Boolean expression evaluated against each record from " + VIEW_BACKING_TOPIC_CONFIG
                    + ". Only records for which the expression evaluates to true are returned to "
                    + "consumers. Supported bindings: body (parsed JSON), headers, key, offset, "
                    + "partition, timestamp. The expression is validated at config-set time and "
                    + "sandboxed at runtime; function calls and regex matchers are rejected.";

    public static final String VIEW_OFFSET_MODE_DOC =
            "Strategy for offsets surfaced through the view. Currently only \""
                    + VIEW_OFFSET_MODE_SOURCE_SPARSE + "\" is supported: records keep their "
                    + "backing-topic offset, so consumer offsets remain comparable with the "
                    + "backing topic and gaps are visible where records were filtered out.";
}
