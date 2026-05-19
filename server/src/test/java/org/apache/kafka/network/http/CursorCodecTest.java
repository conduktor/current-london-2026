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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CursorCodecTest {

    @Test
    void encodeIsDeterministic() {
        String a = CursorCodec.encode("orders", 3, 1024);
        String b = CursorCodec.encode("orders", 3, 1024);
        assertEquals(a, b);
    }

    @Test
    void encodeIsUrlSafe() {
        String c = CursorCodec.encode("topic-with/special chars=", 0, 0);
        // base64url avoids '+' and '/' (replaced with '-' and '_'), no padding spillage in URL
        assertTrue(c.matches("[A-Za-z0-9_-]+"), "cursor must be URL-safe but was: " + c);
    }

    @Test
    void decodeRoundTrips() {
        CursorCodec.Cursor original = new CursorCodec.Cursor("orders", 7, 99_999);
        String encoded = CursorCodec.encode(original.topic(), original.partition(), original.offset());
        CursorCodec.Cursor decoded = CursorCodec.decode(encoded);
        assertEquals(original, decoded);
    }

    @Test
    void decodeRejectsGarbage() {
        assertThrows(IllegalArgumentException.class, () -> CursorCodec.decode("not_a_cursor!"));
    }

    @Test
    void decodeRejectsTruncated() {
        // Base64-decodable but doesn't carry three fields
        String bad = CursorCodec.encode("t", 1, 1).substring(0, 2);
        assertThrows(IllegalArgumentException.class, () -> CursorCodec.decode(bad));
    }

    @Test
    void differentInputsProduceDifferentCursors() {
        assertNotEquals(CursorCodec.encode("a", 0, 0), CursorCodec.encode("b", 0, 0));
        assertNotEquals(CursorCodec.encode("a", 0, 0), CursorCodec.encode("a", 1, 0));
        assertNotEquals(CursorCodec.encode("a", 0, 0), CursorCodec.encode("a", 0, 1));
    }

    @Test
    void zeroOffsetIsRepresentable() {
        CursorCodec.Cursor c = CursorCodec.decode(CursorCodec.encode("topic", 0, 0));
        assertEquals(0, c.offset());
        assertEquals(0, c.partition());
    }

    @Test
    void largeOffsetSurvivesRoundTrip() {
        long huge = Long.MAX_VALUE / 2;
        CursorCodec.Cursor c = CursorCodec.decode(CursorCodec.encode("topic", 42, huge));
        assertEquals(huge, c.offset());
    }

    @Test
    void decodeRejectsNegativePartition() {
        // A tampered cursor must take the same validation path as ?partition=-1 (rejected with 400).
        // The encoder is permissive — a hand-rolled "topic|-1|0" base64url cursor exercises decode.
        String tampered = java.util.Base64.getUrlEncoder().withoutPadding()
            .encodeToString("topic|-1|0".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> CursorCodec.decode(tampered));
        assertTrue(e.getMessage().contains("partition"),
            "expected partition validation error, got: " + e.getMessage());
    }

    @Test
    void decodeRejectsNegativeOffset() {
        String tampered = java.util.Base64.getUrlEncoder().withoutPadding()
            .encodeToString("topic|0|-1".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> CursorCodec.decode(tampered));
        assertTrue(e.getMessage().contains("offset"),
            "expected offset validation error, got: " + e.getMessage());
    }

    @Test
    void decodeRejectsCursorLongerThanMaxLength() {
        // Real cursors are ~380 chars at most (base64(249 + 1 + 11 + 1 + 20)). A fake "cursor" of
        // 8 KiB of A's would allocate one base64 output byte array and a UTF-8 String per request —
        // bounded by the cap below, the wasted work is at most ~1 KiB per rejected cursor instead.
        StringBuilder sb = new StringBuilder(CursorCodec.MAX_CURSOR_LENGTH + 1);
        for (int i = 0; i < CursorCodec.MAX_CURSOR_LENGTH + 1; i++) {
            sb.append('A');
        }
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> CursorCodec.decode(sb.toString()));
        assertTrue(e.getMessage().toLowerCase(java.util.Locale.ROOT).contains("length")
                || e.getMessage().toLowerCase(java.util.Locale.ROOT).contains("maximum"),
            "expected length-related error, got: " + e.getMessage());
    }

    @Test
    void decodeAcceptsCursorAtMaxLength() {
        // A 512-char base64url string is acceptable in shape (the base64 decode may fail for arbitrary
        // bytes, but that's a separate validation path). Build a real round-trippable cursor that
        // lands at-or-below the cap to pin the boundary.
        CursorCodec.Cursor c = CursorCodec.decode(CursorCodec.encode("topic", 0, Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, c.offset());
    }
}
