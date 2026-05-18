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
package org.apache.kafka.server.rules.cel;

/**
 * Thrown for evaluation-time errors that the engine must report rather
 * than silently swallow (e.g., a method invoked on the wrong receiver type
 * in a non-null context). Missing identifier lookups are not surfaced as
 * exceptions — they evaluate to {@code null}, which is falsy in a Boolean
 * context, so DENY rules stay fail-safe over heterogeneous request shapes.
 */
public class CelEvaluationException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public CelEvaluationException(String message) {
        super(message);
    }
}
