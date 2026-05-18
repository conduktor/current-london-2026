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

/**
 * Converts a broker throttle delay (in milliseconds) into the integer-seconds value carried by the {@code Retry-After}
 * HTTP header. The result is always the ceiling: a 1ms throttle still nudges the client to wait at least 1 second,
 * which matches the header's whole-second resolution and avoids advertising "no wait" when there genuinely is one.
 */
public final class RetryAfterCalculator {

    private static final long MILLIS_PER_SECOND = 1000L;

    private RetryAfterCalculator() {
    }

    /** True when a positive throttle was observed and a {@code Retry-After} header should be added. */
    public static boolean shouldSet(long throttleMs) {
        return throttleMs > 0;
    }

    /** Returns the {@code Retry-After} value in seconds (ceiling). Returns 0 for non-positive input. */
    public static int seconds(long throttleMs) {
        if (throttleMs <= 0) {
            return 0;
        }
        long ceil = (throttleMs + MILLIS_PER_SECOND - 1) / MILLIS_PER_SECOND;
        return Math.toIntExact(ceil);
    }

    /**
     * Applies the spec-level policy that combines the throttle hint with the resolved HTTP status:
     * a {@code Retry-After} header is emitted only when the broker reported a positive throttle delay
     * <em>and</em> the status code is one that the spec permits to carry it — i.e. {@code 200} (quota throttle on a
     * successful produce), {@code 207} (mixed multi-status with throttle), or any status for which
     * {@link HttpStatusMapper#statusCarriesRetryAfter(int)} returns true ({@code 503}/{@code 504}). For any other
     * status (notably {@code 400}, {@code 403}, {@code 404}) the throttle hint is dropped — emitting Retry-After
     * there would mislead the client into expecting that retrying eventually succeeds.
     *
     * @return the seconds value to advertise, or {@code 0} to indicate no header should be set
     */
    public static int forStatus(int status, long throttleMs) {
        if (throttleMs <= 0) {
            return 0;
        }
        boolean isOkOrMultiStatus = status == HttpStatusMapper.OK || status == HttpStatusMapper.MULTI_STATUS;
        if (isOkOrMultiStatus || HttpStatusMapper.statusCarriesRetryAfter(status)) {
            return seconds(throttleMs);
        }
        return 0;
    }
}
