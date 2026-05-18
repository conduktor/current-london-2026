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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Broker-side cache of compiled {@link ViewSpec}s, keyed by view-topic name.
 *
 * Compiling a CEL predicate is non-trivial (lex + parse + AST traversal cost-check) and we run
 * it on the fetch hot path for every fetch against the view. The registry caches the compiled
 * form so we pay that cost only when the view's configuration changes.
 *
 * Configuration discovery is delegated to a {@link Function} so the registry stays unit-testable
 * without dragging {@code metadata}/{@code core} dependencies into {@code server-common}. The
 * broker wiring (in {@code core}) supplies a function backed by {@code ConfigRepository}.
 *
 * Thread-safety: lookups are lock-free reads on a {@link ConcurrentHashMap}. Misses fall through
 * to {@link #compile}, which is guarded by {@code computeIfAbsent} so concurrent fetches for the
 * same topic compile exactly once. Configuration updates call {@link #invalidate} to drop the
 * cached entry; the next fetch recompiles from the latest config snapshot.
 */
public final class ViewRegistry {

    private final Function<String, Optional<TopicViewConfigs>> configSource;
    private final PredicateCompiler compiler;
    private final ConcurrentHashMap<String, Optional<ViewSpec>> cache = new ConcurrentHashMap<>();

    /**
     * @param configSource looks up the three view configs for a topic. Returns {@code empty()}
     *                     for non-view topics (any of the three configs missing). The source
     *                     is expected to reflect the broker's live config view — typically
     *                     wraps {@code ConfigRepository.topicConfig(name)}.
     * @param compiler     the CEL compiler. Sharing one compiler across all views is fine —
     *                     it carries only {@link PredicateLimits}, which are immutable.
     */
    public ViewRegistry(Function<String, Optional<TopicViewConfigs>> configSource,
                        PredicateCompiler compiler) {
        this.configSource = Objects.requireNonNull(configSource, "configSource");
        this.compiler = Objects.requireNonNull(compiler, "compiler");
    }

    /**
     * Convenience constructor using the default {@link PredicateLimits}.
     */
    public ViewRegistry(Function<String, Optional<TopicViewConfigs>> configSource) {
        this(configSource, new PredicateCompiler(PredicateLimits.defaults()));
    }

    /**
     * Returns the spec for {@code viewTopic} if and only if {@code viewTopic} is configured as
     * a view (all three configs present). Returns {@code empty()} otherwise — regular topics
     * stay regular.
     *
     * The result of every lookup (hit or miss) is cached, so we don't repeatedly call the
     * config source for non-view topics either. {@link #invalidate} clears entries when
     * configs change.
     *
     * @throws PredicateValidationException if the cached predicate text turns out to be
     *                                      invalid. In practice this cannot happen: the
     *                                      {@code LogConfig} layer rejects bad predicates at
     *                                      config-set time. Wrapping the exception lets us
     *                                      surface the failure to the fetch path in case the
     *                                      validation layer is bypassed.
     */
    public Optional<ViewSpec> viewFor(String viewTopic) {
        Objects.requireNonNull(viewTopic, "viewTopic");
        return cache.computeIfAbsent(viewTopic, this::compile);
    }

    /**
     * Drop the cached entry for {@code viewTopic}. The next call to {@link #viewFor} will
     * re-fetch the configs and recompile.
     *
     * Called by the broker when a topic's config changes (create, alter, delete). Topic deletion
     * also enters this path: KRaft replays {@code RemoveTopicRecord} as an empty-config delta,
     * which the {@code TopicConfigHandler} surfaces to us — so a recreate-with-different-shape
     * cannot leak a stale spec or miss entry. Safe to call for non-view topics; the registry
     * tracks miss entries too.
     *
     * Caller contract: the {@code configSource} threaded into this registry must already reflect
     * the new configs by the time this method is called. The broker satisfies this because the
     * metadata-cache publisher swaps to the new {@code MetadataImage} earlier in the same publish
     * cycle than the dynamic-config publisher that calls this hook. Calling {@code invalidate}
     * before the source is updated would leave a fetch racing with us free to recompile against
     * the *old* configs and re-cache them.
     */
    public void invalidate(String viewTopic) {
        cache.remove(viewTopic);
    }

    /** Drop all cached entries. Used when wholesale config refresh is needed (e.g. broker shutdown). */
    public void clear() {
        cache.clear();
    }

    private Optional<ViewSpec> compile(String viewTopic) {
        Optional<TopicViewConfigs> maybe = configSource.apply(viewTopic);
        if (maybe.isEmpty()) {
            return Optional.empty();
        }
        TopicViewConfigs cfg = maybe.get();
        CompiledPredicate predicate = compiler.compile(cfg.predicate());
        return Optional.of(new ViewSpec(viewTopic, cfg.backingTopic(), predicate, cfg.offsetMode()));
    }

    /**
     * The three view configs, extracted from a topic's full config map. Used as the return
     * shape of the {@link Function} passed to the registry; lets the registry sidestep keying
     * directly into a {@code Map<String,String>} (so callers can do their own validation /
     * defaulting).
     */
    public static final class TopicViewConfigs {
        private final String backingTopic;
        private final String predicate;
        private final String offsetMode;

        public TopicViewConfigs(String backingTopic, String predicate, String offsetMode) {
            this.backingTopic = Objects.requireNonNull(backingTopic, "backingTopic");
            this.predicate = Objects.requireNonNull(predicate, "predicate");
            this.offsetMode = Objects.requireNonNull(offsetMode, "offsetMode");
        }

        /**
         * Extract the three view configs from a topic-level config map, returning
         * {@code empty()} unless all three are present and non-blank. The strict
         * "all-or-none" rule is enforced upstream by {@code LogConfig.validateValues} —
         * this method is a defensive last line.
         */
        public static Optional<TopicViewConfigs> fromMap(Map<String, ?> configs) {
            String backing = stringValue(configs, ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG);
            String predicate = stringValue(configs, ViewTopicConfig.VIEW_CEL_PREDICATE_CONFIG);
            String offsetMode = stringValue(configs, ViewTopicConfig.VIEW_OFFSET_MODE_CONFIG);
            if (backing == null || backing.trim().isEmpty()
                    || predicate == null || predicate.trim().isEmpty()
                    || offsetMode == null || offsetMode.trim().isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new TopicViewConfigs(backing, predicate, offsetMode));
        }

        private static String stringValue(Map<String, ?> configs, String key) {
            if (configs == null) return null;
            Object v = configs.get(key);
            return v == null ? null : v.toString();
        }

        public String backingTopic() {
            return backingTopic;
        }

        public String predicate() {
            return predicate;
        }

        public String offsetMode() {
            return offsetMode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TopicViewConfigs)) return false;
            TopicViewConfigs that = (TopicViewConfigs) o;
            return backingTopic.equals(that.backingTopic)
                    && predicate.equals(that.predicate)
                    && offsetMode.equals(that.offsetMode);
        }

        @Override
        public int hashCode() {
            return Objects.hash(backingTopic, predicate, offsetMode);
        }
    }
}
