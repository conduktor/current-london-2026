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

import java.io.IOException;

/**
 * Thrown by {@link BoundedRequestBody} when an inbound request body would exceed the configured limit.
 *
 * <p>Extends {@link IOException} so the exception surfaces cleanly through Jackson's stream-parsing call stack — Jackson
 * wraps {@code IOException} as-is, so the catcher in {@link KafkaHttpServlet} can recognise the cap-exceeded case and
 * emit {@code HTTP 413 Payload Too Large} rather than {@code 500}.
 *
 * <p>{@link #limit()} carries the configured byte cap so the error envelope can quote the limit back to the caller; a
 * client otherwise has no way to know how big "too large" actually is.
 */
public final class BodyTooLargeException extends IOException {

    private static final long serialVersionUID = 1L;

    private final long limit;

    public BodyTooLargeException(long limit) {
        super("request body exceeds the configured limit of " + limit + " bytes");
        this.limit = limit;
    }

    public long limit() {
        return limit;
    }
}
