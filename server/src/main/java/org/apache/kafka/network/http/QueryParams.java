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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Servlet-API-free view of a request's query string. Empty or whitespace-only values are treated as absent so the
 * parsers don't have to special-case clients that send {@code ?partition=&offset=}.
 *
 * <p>The {@link #of(String...)} factory is for tests; production code uses {@link #from(Map)} on a servlet
 * parameter map. Keeping the bridge decoupled from the servlet API here lets us unit-test the parsers without
 * spinning up Jetty.
 */
public final class QueryParams {

    private final Map<String, String> values;

    private QueryParams(Map<String, String> values) {
        this.values = values;
    }

    public Optional<String> get(String key) {
        String raw = values.get(key);
        if (raw == null) {
            return Optional.empty();
        }
        String trimmed = raw.trim();
        return trimmed.isEmpty() ? Optional.empty() : Optional.of(trimmed);
    }

    public static QueryParams of(String... pairs) {
        if ((pairs.length & 1) != 0) {
            throw new IllegalArgumentException("QueryParams.of requires an even number of arguments");
        }
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put(pairs[i], pairs[i + 1]);
        }
        return new QueryParams(map);
    }

    public static QueryParams from(Map<String, String[]> servletParameterMap) {
        Map<String, String> map = new HashMap<>();
        if (servletParameterMap != null) {
            for (Map.Entry<String, String[]> e : servletParameterMap.entrySet()) {
                String[] arr = e.getValue();
                if (arr != null && arr.length > 0) {
                    map.put(e.getKey(), arr[0]);
                }
            }
        }
        return new QueryParams(map);
    }
}
