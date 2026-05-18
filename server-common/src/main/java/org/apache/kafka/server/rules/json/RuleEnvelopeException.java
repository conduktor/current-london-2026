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
package org.apache.kafka.server.rules.json;

/**
 * Thrown by {@link RuleJsonCodec} when an envelope read from the
 * {@code __governance} compacted topic cannot be turned into a valid
 * {@code Rule}: bad JSON, unknown api-key name, unsupported action,
 * uncompilable CEL, or missing required fields.
 *
 * <p>The loader catches this per-record and preserves the previously-good
 * {@code RuleSet}, so a single bad envelope does not corrupt state.
 */
public class RuleEnvelopeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public RuleEnvelopeException(String message) {
        super(message);
    }

    public RuleEnvelopeException(String message, Throwable cause) {
        super(message, cause);
    }
}
