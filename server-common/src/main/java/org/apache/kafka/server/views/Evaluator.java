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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Tree-walking AST interpreter. One {@link #evaluate(Ast.Node, RecordContext)} call processes a
 * single record. Per-record step counter enforces the cost cap; exceeding it returns the
 * sentinel {@link #SKIP} which propagates upward and causes the top-level predicate to skip
 * the record.
 *
 * Values exchanged inside the evaluator are one of:
 *  - {@code Long}            (integer literal or JSON number with no fractional part)
 *  - {@code Double}          (float literal or JSON float)
 *  - {@code String}          (string literal or UTF-8-decoded value)
 *  - {@code Boolean}
 *  - {@code null}            (JSON null / missing field / UTF-8 decode failure)
 *  - {@link #SKIP}           (cost-cap exceeded or unrecoverable error — propagates upward)
 *
 * Type mismatches in comparisons (e.g. string {@literal <} number) yield {@code null} rather
 * than {@link #SKIP}: the value is "unknown", not "give up". Logical short-circuit on
 * {@code &&}/{@code ||} treats {@code null} operands as falsy.
 */
final class Evaluator {

    /** Sentinel value: stop evaluating and skip the record. */
    static final Object SKIP = new Object();

    private final PredicateLimits limits;
    // Per-evaluation mutable state. The Evaluator instance itself is not shared between
    // threads — a fresh one is created per evaluate() call by CompiledPredicate.
    private int steps = 0;

    Evaluator(PredicateLimits limits) {
        this.limits = limits;
    }

    Object evaluate(Ast.Node n, RecordContext ctx) {
        if (++steps > limits.maxStepsPerEval) return SKIP;
        if (n instanceof Ast.Literal) {
            return ((Ast.Literal) n).value;
        }
        if (n instanceof Ast.Path) {
            return resolvePath((Ast.Path) n, ctx);
        }
        if (n instanceof Ast.Unary) {
            return evalUnary((Ast.Unary) n, ctx);
        }
        if (n instanceof Ast.Binary) {
            return evalBinary((Ast.Binary) n, ctx);
        }
        return SKIP;
    }

    private Object evalUnary(Ast.Unary u, RecordContext ctx) {
        Object v = evaluate(u.operand, ctx);
        if (v == SKIP) return SKIP;
        switch (u.op) {
            case NOT:
                if (v instanceof Boolean) return !((Boolean) v);
                return null; // non-boolean → unknown
            case NEG:
                if (v instanceof Long) return -((Long) v);
                if (v instanceof Double) return -((Double) v);
                return null;
            default:
                return SKIP;
        }
    }

    private Object evalBinary(Ast.Binary b, RecordContext ctx) {
        // Logical operators short-circuit and treat non-boolean operands as falsy.
        if (b.op == Ast.Binary.Op.AND || b.op == Ast.Binary.Op.OR) {
            return evalLogical(b, ctx);
        }

        Object lv = evaluate(b.left, ctx);
        if (lv == SKIP) {
            return SKIP;
        }
        Object rv = evaluate(b.right, ctx);
        if (rv == SKIP) {
            return SKIP;
        }
        return applyBinaryOp(b.op, lv, rv);
    }

    private Object evalLogical(Ast.Binary b, RecordContext ctx) {
        boolean isAnd = b.op == Ast.Binary.Op.AND;
        Object l = evaluate(b.left, ctx);
        if (l == SKIP) {
            return SKIP;
        }
        boolean leftTruthy = truthy(l);
        if (isAnd && !leftTruthy) {
            return Boolean.FALSE;
        }
        if (!isAnd && leftTruthy) {
            return Boolean.TRUE;
        }
        Object r = evaluate(b.right, ctx);
        if (r == SKIP) {
            return SKIP;
        }
        return Boolean.valueOf(truthy(r));
    }

    private Object applyBinaryOp(Ast.Binary.Op op, Object lv, Object rv) {
        switch (op) {
            case EQ:
                return Boolean.valueOf(equalsValues(lv, rv));
            case NEQ:
                return Boolean.valueOf(!equalsValues(lv, rv));
            case LT:
                return compare(lv, rv, -1, false);
            case LTE:
                return compare(lv, rv, -1, true);
            case GT:
                return compare(lv, rv, 1, false);
            case GTE:
                return compare(lv, rv, 1, true);
            case ADD:
                return arith(lv, rv, Op.ADD);
            case SUB:
                return arith(lv, rv, Op.SUB);
            case MUL:
                return arith(lv, rv, Op.MUL);
            case DIV:
                return arith(lv, rv, Op.DIV);
            case MOD:
                return arith(lv, rv, Op.MOD);
            default:
                return SKIP;
        }
    }

    private static boolean truthy(Object v) {
        return v instanceof Boolean && (Boolean) v;
    }

    /** 2^53 — largest integer that all IEEE-754 doubles can represent exactly. A {@code long}
     *  beyond this range loses precision when promoted to {@code double}, so mixed-type
     *  equality and ordered comparison must refuse rather than silently match nearby values. */
    private static final long IEEE_SAFE_INTEGER = 1L << 53;

    /**
     * Equality across numeric types: 1 == 1.0 is true. String == number is false (not unknown)
     * because consumers reasonably expect equality to be total. JSON-null / missing operand
     * compares unequal to any non-null value, equal to itself.
     *
     * <p>For mixed Long/Double equality the operands are only compared in {@code double} space
     * when the Long is inside the IEEE-safe-integer range ({@code |L| <= 2^53}). Outside that
     * range the equality returns {@code false}: predicates are an access-control boundary, and
     * naïve {@code (double) L == D} comparison would let an adversary craft Long values that
     * "equal" the rounded double representation of a different literal (e.g. body Long
     * {@code 2^53 + 1} matching predicate Double {@code 2^53.0}).
     */
    private static boolean equalsValues(Object l, Object r) {
        if (l == null && r == null) return true;
        if (l == null || r == null) return false;
        if (l instanceof Number && r instanceof Number) {
            return equalsNumeric((Number) l, (Number) r);
        }
        if (l instanceof Boolean && r instanceof Boolean) return l.equals(r);
        if (l instanceof String && r instanceof String) return l.equals(r);
        return false;
    }

    private static boolean equalsNumeric(Number l, Number r) {
        if (l instanceof Long && r instanceof Long) {
            return ((Long) l).longValue() == ((Long) r).longValue();
        }
        if (l instanceof Long) {
            return numericEqLongDouble((Long) l, r.doubleValue());
        }
        if (r instanceof Long) {
            return numericEqLongDouble((Long) r, l.doubleValue());
        }
        // Both Double.
        return l.doubleValue() == r.doubleValue();
    }

    private static boolean numericEqLongDouble(long longSide, double doubleSide) {
        if (Double.isNaN(doubleSide) || Double.isInfinite(doubleSide)) return false;
        if (longSide > IEEE_SAFE_INTEGER || longSide < -IEEE_SAFE_INTEGER) {
            // Long is outside the range where (double) longSide is exact. Refuse to match
            // rather than silently equate to a rounded value.
            return false;
        }
        return (double) longSide == doubleSide;
    }

    /**
     * Ordered comparison: returns Boolean for valid numeric (or string-vs-string) compares,
     * null for type mismatches (treated as "unknown", which propagates as falsy in boolean
     * context).
     *
     * <p>For mixed Long/Double comparisons the same IEEE-safe-integer guard used by
     * {@link #equalsValues} applies: a Long outside {@code |L| <= 2^53} cannot be compared in
     * {@code double} space without precision loss, so the comparison returns {@code null}
     * rather than silently using a rounded value. Without this guard, a body Long
     * {@code 2^53 + 1} would compare {@code <= 2^53.0} as true (the rounded double of the Long
     * equals the literal), letting an adversary bypass an {@code account_id <= 2^53.0}
     * gate.
     */
    private Object compare(Object l, Object r, int target, boolean inclusive) {
        if (l == null || r == null) return null;
        if (l instanceof Number && r instanceof Number) {
            Integer cmp = compareNumeric((Number) l, (Number) r);
            return cmp == null ? null : matches(cmp, target, inclusive);
        }
        if (l instanceof String && r instanceof String) {
            int cmp = Integer.signum(((String) l).compareTo((String) r));
            return matches(cmp, target, inclusive);
        }
        return null;
    }

    private static Integer compareNumeric(Number l, Number r) {
        if (l instanceof Long && r instanceof Long) {
            return Long.compare((Long) l, (Long) r);
        }
        if (l instanceof Long) {
            return compareLongDouble((Long) l, r.doubleValue(), false);
        }
        if (r instanceof Long) {
            return compareLongDouble((Long) r, l.doubleValue(), true);
        }
        return Double.compare(l.doubleValue(), r.doubleValue());
    }

    /** Compare a Long against a Double under the IEEE-safe-integer guard. {@code longOnRight=true}
     *  swaps the comparison so the result is relative to the original left-hand operand. */
    private static Integer compareLongDouble(long longSide, double doubleSide, boolean longOnRight) {
        if (Double.isNaN(doubleSide) || Double.isInfinite(doubleSide)) {
            return null;
        }
        if (longSide > IEEE_SAFE_INTEGER || longSide < -IEEE_SAFE_INTEGER) {
            return null;
        }
        // Swap operands rather than negating the result: Double.compare doesn't promise the
        // {-1,0,+1} contract that would make negation safe, and SpotBugs flags negated compares
        // (RV_NEGATING_RESULT_OF_COMPARETO).
        return longOnRight
                ? Double.compare(doubleSide, (double) longSide)
                : Double.compare((double) longSide, doubleSide);
    }

    private static Boolean matches(int cmp, int target, boolean inclusive) {
        cmp = Integer.signum(cmp);
        if (inclusive && cmp == 0) return Boolean.TRUE;
        return cmp == target;
    }

    private enum Op { ADD, SUB, MUL, DIV, MOD }

    private Object arith(Object l, Object r, Op op) {
        if (!(l instanceof Number) || !(r instanceof Number)) {
            return null;
        }
        boolean wantDouble = l instanceof Double || r instanceof Double;
        if (wantDouble) {
            return arithDouble(((Number) l).doubleValue(), ((Number) r).doubleValue(), op);
        }
        return arithLong(((Number) l).longValue(), ((Number) r).longValue(), op);
    }

    private static Object arithDouble(double a, double b, Op op) {
        double v;
        switch (op) {
            case ADD:
                v = a + b;
                break;
            case SUB:
                v = a - b;
                break;
            case MUL:
                v = a * b;
                break;
            case DIV:
                if (b == 0.0) {
                    return null;
                }
                v = a / b;
                break;
            case MOD:
                if (b == 0.0) {
                    return null;
                }
                v = a % b;
                break;
            default:
                return null;
        }
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            return null;
        }
        return v;
    }

    private static Object arithLong(long a, long b, Op op) {
        try {
            switch (op) {
                case ADD:
                    return Math.addExact(a, b);
                case SUB:
                    return Math.subtractExact(a, b);
                case MUL:
                    return Math.multiplyExact(a, b);
                case DIV:
                    if (b == 0) {
                        return null;
                    }
                    return a / b;
                case MOD:
                    if (b == 0) {
                        return null;
                    }
                    return a % b;
                default:
                    return null;
            }
        } catch (ArithmeticException overflow) {
            // long overflow on +/-/* → propagate as "unknown" rather than crashing the broker.
            return null;
        }
    }

    private Object resolvePath(Ast.Path path, RecordContext ctx) {
        switch (path.root) {
            case "offset":
                if (!path.accessors.isEmpty()) return null;
                return ctx.offset();
            case "partition":
                if (!path.accessors.isEmpty()) return null;
                return (long) ctx.partition(); // promote to long for uniform numeric handling
            case "timestamp":
                if (!path.accessors.isEmpty()) return null;
                return ctx.timestamp();
            case "key":
                return resolveKey(path, ctx);
            case "headers":
                return resolveHeader(path, ctx);
            case "body": {
                List<String> tail = new ArrayList<>(path.accessors);
                Object v = ctx.bodyAt(tail);
                // Identity-compare against the sentinel: body unusable → SKIP. Anything else
                // (including null = "missing/unknown") propagates as-is.
                if (v == RecordContext.BODY_UNUSABLE) {
                    return SKIP;
                }
                return v;
            }
            default:
                // Should never reach here — parser rejects unknown roots.
                return null;
        }
    }

    /**
     * Resolves the {@code key} binding with absent-vs-undecodable disambiguation. Empty
     * {@link Optional} from {@link RecordContext#keyAsString} can mean (1) the record has no key
     * — legitimate null, predicate {@code key == 'x'} evaluates to false, {@code key == null}
     * to true — or (2) key bytes are present but not valid UTF-8 (UNDECODABLE — per PROMPT.md
     * scenario "invalid UTF-8 ... silently skips those records" we return SKIP, otherwise
     * {@code key != 'blocked'} would falsely retain the record).
     */
    private Object resolveKey(Ast.Path path, RecordContext ctx) {
        if (!path.accessors.isEmpty()) return null;
        Optional<String> decoded = ctx.keyAsString();
        if (decoded.isPresent()) return decoded.get();
        return ctx.rawKey() == null ? null : SKIP;
    }

    /** Same absent-vs-undecodable distinction as {@link #resolveKey} for header accessors. */
    private Object resolveHeader(Ast.Path path, RecordContext ctx) {
        if (path.accessors.size() != 1) return null;
        String name = path.accessors.get(0);
        Optional<String> decoded = ctx.header(name);
        if (decoded.isPresent()) return decoded.get();
        return ctx.rawHeader(name) == null ? null : SKIP;
    }
}
