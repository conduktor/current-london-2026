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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryAfterCalculatorTest {

    @Test
    void zeroThrottleHasNoRetryAfter() {
        assertFalse(RetryAfterCalculator.shouldSet(0));
        assertEquals(0, RetryAfterCalculator.seconds(0));
    }

    @Test
    void negativeThrottleHasNoRetryAfter() {
        // Defensive: a negative value cannot represent a wait time
        assertFalse(RetryAfterCalculator.shouldSet(-1));
        assertFalse(RetryAfterCalculator.shouldSet(-1000));
    }

    @Test
    void positiveThrottleSetsRetryAfter() {
        assertTrue(RetryAfterCalculator.shouldSet(1));
        assertTrue(RetryAfterCalculator.shouldSet(1500));
    }

    @Test
    void oneMillisecondCeilsToOneSecond() {
        assertEquals(1, RetryAfterCalculator.seconds(1));
    }

    @Test
    void exactlyOneSecondReturnsOne() {
        assertEquals(1, RetryAfterCalculator.seconds(1000));
    }

    @Test
    void onePointOneSecondCeilsToTwo() {
        assertEquals(2, RetryAfterCalculator.seconds(1100));
    }

    @Test
    void onePointFiveSecondCeilsToTwo() {
        assertEquals(2, RetryAfterCalculator.seconds(1500));
    }

    @Test
    void exactlyTwoSecondsReturnsTwo() {
        assertEquals(2, RetryAfterCalculator.seconds(2000));
    }

    @Test
    void largeThrottleCeilsCorrectly() {
        assertEquals(60, RetryAfterCalculator.seconds(59_999));
        assertEquals(60, RetryAfterCalculator.seconds(60_000));
        assertEquals(61, RetryAfterCalculator.seconds(60_001));
    }
}
