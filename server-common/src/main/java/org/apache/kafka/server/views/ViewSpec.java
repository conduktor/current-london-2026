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

import java.util.Objects;

/**
 * Immutable description of a view topic.
 *
 * A {@code ViewSpec} is the compiled form of the three view configs ({@code view.backing.topic},
 * {@code view.cel.predicate}, {@code view.offset.mode}) — the broker keeps one per active view
 * inside {@link ViewRegistry}. The compiled predicate is reused across every fetch, so we
 * never re-parse the CEL text on the hot path.
 *
 * <h2>Partition-assignment constraint</h2>
 *
 * <p>Filtering happens on the broker that serves the view's fetch — that is, the leader of the
 * view's partition. For the broker to read the backing partition locally it must also host that
 * backing partition (as leader or follower replica). Concretely:
 *
 * <ul>
 *   <li>The view topic and the backing topic must have the <em>same number of partitions</em>;
 *       fetches for view partition {@code N} are rewritten to backing partition {@code N}.</li>
 *   <li>For every partition {@code N}, the leader of view partition {@code N} must also be a
 *       replica of backing partition {@code N}. The simplest way to guarantee this is to give
 *       view and backing the <em>same replica assignment</em> (admin command: create the view
 *       with {@code --replica-assignment} matching the backing topic).</li>
 * </ul>
 *
 * <p>If this constraint is violated at fetch time — for example, the view's leader has moved to
 * a broker that does not replicate the backing partition — the broker fails the fetch with
 * {@code UNKNOWN_TOPIC_OR_PARTITION} keyed at the view (see {@code KafkaApis.handleFetchRequest}
 * routing pass). Cross-broker proxying is intentionally <em>not</em> implemented: it would turn
 * every view fetch into a second hop, defeat zero-copy on the receiving broker, and require new
 * inter-broker auth flows for predicate-filtered traffic. The constraint is documented and
 * enforced by the operator-facing config tooling instead.</p>
 */
public final class ViewSpec {

    private final String viewTopic;
    private final String backingTopic;
    private final CompiledPredicate predicate;
    private final String offsetMode;

    public ViewSpec(String viewTopic,
                    String backingTopic,
                    CompiledPredicate predicate,
                    String offsetMode) {
        this.viewTopic = Objects.requireNonNull(viewTopic, "viewTopic");
        this.backingTopic = Objects.requireNonNull(backingTopic, "backingTopic");
        this.predicate = Objects.requireNonNull(predicate, "predicate");
        this.offsetMode = Objects.requireNonNull(offsetMode, "offsetMode");
        if (viewTopic.equals(backingTopic)) {
            throw new IllegalArgumentException(
                    "A view cannot back itself: viewTopic = backingTopic = " + viewTopic);
        }
        if (!ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE.equals(offsetMode)) {
            throw new IllegalArgumentException(
                    "Unsupported offset mode: " + offsetMode + " (only "
                            + ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE + " is supported)");
        }
    }

    public String viewTopic() {
        return viewTopic;
    }

    public String backingTopic() {
        return backingTopic;
    }

    public CompiledPredicate predicate() {
        return predicate;
    }

    public String offsetMode() {
        return offsetMode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ViewSpec)) return false;
        ViewSpec that = (ViewSpec) o;
        return viewTopic.equals(that.viewTopic)
                && backingTopic.equals(that.backingTopic)
                && offsetMode.equals(that.offsetMode)
                && Objects.equals(predicate.predicateText(), that.predicate.predicateText());
    }

    @Override
    public int hashCode() {
        return Objects.hash(viewTopic, backingTopic, offsetMode, predicate.predicateText());
    }

    @Override
    public String toString() {
        return "ViewSpec(viewTopic=" + viewTopic
                + ", backingTopic=" + backingTopic
                + ", offsetMode=" + offsetMode
                + ", predicate=" + predicate.predicateText()
                + ")";
    }
}
