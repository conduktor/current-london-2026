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

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertTrue;

class PredicateCompilerPropertyTest {

    private final PredicateCompiler compiler = new PredicateCompiler(PredicateLimits.defaults());

    @Property(tries = 500)
    void randomValidPredicateAndJsonRecordNeverThrow(@ForAll Random random) {
        String predicateText = boolExpr(random, 3);
        String json = jsonObject(random, 3);
        try {
            CompiledPredicate predicate = compiler.compile(predicateText);
            Optional<Boolean> result = predicate.evaluate(RecordContexts.builder()
                    .body(json.getBytes(StandardCharsets.UTF_8))
                    .key(randomString(random).getBytes(StandardCharsets.UTF_8))
                    .header("h", randomString(random).getBytes(StandardCharsets.UTF_8))
                    .offset(random.nextInt(10_000))
                    .partition(random.nextInt(32))
                    .timestamp(Math.abs(random.nextLong()))
                    .build());
            assertTrue(result.isEmpty()
                            || Boolean.TRUE.equals(result.get())
                            || Boolean.FALSE.equals(result.get()),
                    () -> "unexpected predicate result " + result
                            + " for predicate [" + predicateText + "] and json [" + json + "]");
        } catch (RuntimeException | AssertionError e) {
            throw new AssertionError("predicate [" + predicateText + "] against json [" + json + "] threw", e);
        }
    }

    private static String boolExpr(Random random, int depth) {
        if (depth == 0) {
            return oneOf(random,
                    "true",
                    "false",
                    comparison(random));
        }
        switch (random.nextInt(5)) {
            case 0:
                return comparison(random);
            case 1:
                return "!(" + boolExpr(random, depth - 1) + ")";
            case 2:
                return "(" + boolExpr(random, depth - 1) + ") && (" + boolExpr(random, depth - 1) + ")";
            case 3:
                return "(" + boolExpr(random, depth - 1) + ") || (" + boolExpr(random, depth - 1) + ")";
            default:
                return oneOf(random, "true", "false", path(random));
        }
    }

    private static String comparison(Random random) {
        return valueExpr(random, 2) + " " + oneOf(random, "==", "!=", "<", "<=", ">", ">=")
                + " " + valueExpr(random, 2);
    }

    private static String valueExpr(Random random, int depth) {
        if (depth == 0) {
            return oneOf(random,
                    path(random),
                    numberLiteral(random),
                    stringLiteral(random),
                    "true",
                    "false",
                    "null");
        }
        switch (random.nextInt(4)) {
            case 0:
                return "(" + valueExpr(random, depth - 1) + " "
                        + oneOf(random, "+", "-", "*", "/", "%") + " "
                        + valueExpr(random, depth - 1) + ")";
            case 1:
                return "-(" + valueExpr(random, depth - 1) + ")";
            default:
                return valueExpr(random, 0);
        }
    }

    private static String path(Random random) {
        return oneOf(random,
                "body",
                "body.a",
                "body.b",
                "body.c",
                "body.flag",
                "body.s",
                "body.nested.a",
                "body.nested.b",
                "body['quoted-key']",
                "headers.h",
                "headers['h']",
                "key",
                "offset",
                "partition",
                "timestamp");
    }

    private static String numberLiteral(Random random) {
        switch (random.nextInt(5)) {
            case 0:
                return "0";
            case 1:
                return Long.toString(random.nextInt(10_000));
            case 2:
                return Long.toString((long) random.nextInt(10_000) * random.nextInt(10_000));
            case 3:
                return random.nextInt(1_000) + ".5";
            default:
                return random.nextInt(1_000) + ".25";
        }
    }

    private static String stringLiteral(Random random) {
        return "'" + escapePredicateString(randomString(random)) + "'";
    }

    private static String jsonObject(Random random, int depth) {
        return "{"
                + "\"a\":" + jsonValue(random, depth - 1) + ","
                + "\"b\":" + jsonValue(random, depth - 1) + ","
                + "\"c\":" + jsonValue(random, depth - 1) + ","
                + "\"flag\":" + (random.nextBoolean() ? "true" : "false") + ","
                + "\"s\":\"" + escapeJsonString(randomString(random)) + "\","
                + "\"quoted-key\":" + jsonValue(random, depth - 1) + ","
                + "\"nested\":" + (depth <= 1 ? "null" : jsonObject(random, depth - 1))
                + "}";
    }

    private static String jsonValue(Random random, int depth) {
        if (depth <= 0) {
            return jsonScalar(random);
        }
        switch (random.nextInt(7)) {
            case 0:
                return jsonScalar(random);
            case 1:
                return jsonObject(random, depth - 1);
            case 2:
                return "[" + jsonScalar(random) + "," + jsonScalar(random) + "]";
            default:
                return jsonScalar(random);
        }
    }

    private static String jsonScalar(Random random) {
        switch (random.nextInt(10)) {
            case 0:
                return "null";
            case 1:
                return random.nextBoolean() ? "true" : "false";
            case 2:
                return Long.toString(random.nextInt(10_000));
            case 3:
                return "-" + random.nextInt(10_000);
            case 4:
                return random.nextInt(1_000) + ".5";
            case 5:
                return "9223372036854775808";
            case 6:
                return "1e9999";
            case 7:
                return "1e-324";
            case 8:
                return "9.007199254740993e15";
            default:
                return "\"" + escapeJsonString(randomString(random)) + "\"";
        }
    }

    private static String randomString(Random random) {
        return oneOf(random, "", "red", "blue", "tenant", "line\\nfeed", "quote'", "backslash\\");
    }

    private static String escapePredicateString(String value) {
        return value
                .replace("\\", "\\\\")
                .replace("'", "\\'")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private static String escapeJsonString(String value) {
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private static String oneOf(Random random, String... values) {
        return values[random.nextInt(values.length)];
    }
}
