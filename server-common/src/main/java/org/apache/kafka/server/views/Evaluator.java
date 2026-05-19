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

import java.math.BigDecimal;
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
                // Non-boolean operand → SKIP. Returning null here would feed
                // equalsValuesOrNull's "null vs non-null → FALSE" branch, which NEQ negates into a
                // confident TRUE. A predicate like `!body.tenant != true` with body.tenant a
                // string would then admit the record. SKIP propagates through evalBinary's SKIP
                // guard and through evalLogical's OR/AND rescue.
                return SKIP;
            case NEG:
                if (v instanceof Long) {
                    long lv = (Long) v;
                    // -Long.MIN_VALUE silently wraps to Long.MIN_VALUE in Java 2's-complement.
                    // arithLong already converts equivalent overflows on +/-/* to SKIP via
                    // Math.*Exact; NEG must do the same so predicates like
                    // `-body.priority != -Long.MIN_VALUE` don't admit a Long.MIN_VALUE payload
                    // through the wrap-then-NEQ-via-null bypass. SKIP (not null) is required:
                    // null would reach equalsValuesOrNull's null-vs-non-null FALSE branch and
                    // NEQ would negate it into a confident TRUE.
                    if (lv == Long.MIN_VALUE) return SKIP;
                    return -lv;
                }
                if (v instanceof Double) return -((Double) v);
                // Non-numeric operand (string, boolean, null) → SKIP for the same reason as the
                // NOT branch above. E.g. `-body.amount != 0` with body.amount="blocked" must
                // drop the record, not admit it.
                return SKIP;
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
        // PROMPT.md acceptance: "silently skips those records without hiding valid records from
        // unrelated predicates." Applied inside a single predicate, this means a SKIP coming from
        // ONE branch must not poison a result the OTHER branch could have answered definitively.
        // Concretely: `body.bad == "x" || true` is `true`, not SKIP; `body.bad == "x" && false`
        // is `false`, not SKIP. We achieve that by evaluating both branches and only returning
        // SKIP when no determinate short-circuit answer is available.
        boolean isAnd = b.op == Ast.Binary.Op.AND;
        Object l = evaluate(b.left, ctx);
        // Determinate left-side short-circuit: don't evaluate the right side at all.
        if (l != SKIP) {
            boolean leftTruthy = truthy(l);
            if (isAnd && !leftTruthy) {
                return Boolean.FALSE;
            }
            if (!isAnd && leftTruthy) {
                return Boolean.TRUE;
            }
        }
        Object r = evaluate(b.right, ctx);
        // Determinate right-side short-circuit: cover the case where left was SKIP. For OR a
        // truthy right rescues the predicate; for AND a falsy right rescues it.
        if (r != SKIP) {
            boolean rightTruthy = truthy(r);
            if (isAnd && !rightTruthy) {
                return Boolean.FALSE;
            }
            if (!isAnd && rightTruthy) {
                return Boolean.TRUE;
            }
        }
        // Neither side gave a short-circuit rescue. If either side SKIPped, the combined result
        // is SKIP (we can't decide); otherwise both sides are determinate non-rescuing values
        // (AND with both truthy, OR with both falsy) so the right value's truthiness wins.
        if (l == SKIP || r == SKIP) {
            return SKIP;
        }
        return Boolean.valueOf(truthy(r));
    }

    private Object applyBinaryOp(Ast.Binary.Op op, Object lv, Object rv) {
        switch (op) {
            case EQ: {
                // Tri-state: null means "unknown" (e.g. unsafe Long/Double precision-loss).
                // We convert null → SKIP at the operator boundary so it cannot be wrapped in
                // an OUTER binary operator and bypass the guard. R36 (Codex BLOCKER #1):
                // returning bare null here let `(body.x == LIT) != true` admit a record whose
                // inner equality was undecidable, because the outer NEQ ran
                // equalsValuesOrNull(null, true) and the line-231 null-vs-non-null FALSE branch
                // got NEQ-flipped to a confident TRUE. SKIP propagates through evalBinary
                // (lines 114-122) so wrapping cannot launder it.
                Boolean eq = equalsValuesOrNull(lv, rv);
                return eq == null ? SKIP : eq;
            }
            case NEQ: {
                Boolean eq = equalsValuesOrNull(lv, rv);
                return eq == null ? SKIP : Boolean.valueOf(!eq.booleanValue());
            }
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
     * <p>Returns a nullable {@link Boolean}: {@code null} means "unknown" (today only the unsafe
     * mixed Long/Double path produces it). The caller propagates {@code null} to both EQ and NEQ
     * so an undecidable equality cannot be negated into a confident TRUE. Without tri-state, the
     * round-4 IEEE_SAFE_INTEGER guard that encoded the unsafe case as FALSE would let
     * {@code body.x != 9007199254740992.0} admit a Long body {@code 9007199254740993} (precision
     * round-trips to {@code 9007199254740992.0} as a double), bypassing the predicate writer's
     * intent.
     *
     * <p>For mixed Long/Double equality the operands are compared in {@code double} space only
     * when the Long is inside the IEEE-safe-integer range ({@code |L| <= 2^53}); outside that
     * range we return {@code null}.
     */
    private static Boolean equalsValuesOrNull(Object l, Object r) {
        if (l == null && r == null) return Boolean.TRUE;
        if (l == null || r == null) return Boolean.FALSE;
        if (l instanceof Number && r instanceof Number) {
            return equalsNumericOrNull((Number) l, (Number) r);
        }
        if (l instanceof Boolean && r instanceof Boolean) return Boolean.valueOf(l.equals(r));
        if (l instanceof String && r instanceof String) return Boolean.valueOf(l.equals(r));
        return Boolean.FALSE;
    }

    private static Boolean equalsNumericOrNull(Number l, Number r) {
        if (l instanceof Long && r instanceof Long) {
            return Boolean.valueOf(((Long) l).longValue() == ((Long) r).longValue());
        }
        if (l instanceof Long) {
            return numericEqLongDoubleOrNull((Long) l, r.doubleValue());
        }
        if (r instanceof Long) {
            return numericEqLongDoubleOrNull((Long) r, l.doubleValue());
        }
        // Both Double.
        return Boolean.valueOf(l.doubleValue() == r.doubleValue());
    }

    private static Boolean numericEqLongDoubleOrNull(long longSide, double doubleSide) {
        if (Double.isNaN(doubleSide) || Double.isInfinite(doubleSide)) return Boolean.FALSE;
        if (longSide > IEEE_SAFE_INTEGER || longSide < -IEEE_SAFE_INTEGER) {
            // Long is outside the range where (double) longSide is exact. Tri-state UNKNOWN so
            // both EQ and NEQ refuse to commit — a confident FALSE here would let NEQ flip
            // to TRUE and admit a record whose Long value rounds to the predicate's double.
            return null;
        }
        return Boolean.valueOf((double) longSide == doubleSide);
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
            // compareNumeric returns null ONLY from compareLongDouble's NaN/Inf or unsafe-Long
            // branches — both are computation failures on present operands, not absence. SKIP,
            // not null: `(body.unsafe_long <= 9007199254740992.0) != true` would otherwise admit
            // through the same null-vs-non-null FALSE → NEQ-confident-TRUE shape closed by R34
            // for unary/arith/resolvePath. The R5 fix (commit 2ecadee733) tri-stated EQUALITY
            // null safely (equalsValuesOrNull null propagates through NEQ as null, not via
            // line-221 FALSE), but ORDERED comparison's null was only safe for direct boolean
            // use; wrapping it in NEQ re-opens the bypass.
            return cmp == null ? SKIP : matches(cmp, target, inclusive);
        }
        if (l instanceof String && r instanceof String) {
            int cmp = Integer.signum(((String) l).compareTo((String) r));
            return matches(cmp, target, inclusive);
        }
        // Type mismatch (both operands present but incompatible — e.g. string vs number, boolean
        // vs number). SKIP, not null: the value of `body.name < 5` with body.name="blocked" must
        // not be flippable via `(body.name < 5) != true` into a confident TRUE. The null/absent
        // case (one operand null) above keeps returning null so absent-field semantics matched
        // by `absentFieldEvaluatesAsNullForNegatedPredicate` remain pinned.
        return SKIP;
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
        return ieeeCompare(l.doubleValue(), r.doubleValue());
    }

    /** Compare a Long against a Double under the IEEE-safe-integer guard. {@code longOnRight=true}
     *  swaps the comparison so the result is relative to the original left-hand operand.
     *
     *  <p>Returns {@code null} on a computation failure (NaN/Inf operand, or Long out of IEEE-safe
     *  range). The caller {@link #compare} turns the {@code null} from this helper into
     *  {@code SKIP} so an outer {@code (body.x < 5.0) != true} cannot flip a null result via
     *  equalsValuesOrNull's null-vs-non-null FALSE branch into a confident TRUE — the same R3/R4
     *  unary, R33b arith, and R34 unary/resolvePath bypass shape applied to compare-result. */
    private static Integer compareLongDouble(long longSide, double doubleSide, boolean longOnRight) {
        if (Double.isNaN(doubleSide) || Double.isInfinite(doubleSide)) {
            return null;
        }
        if (longSide > IEEE_SAFE_INTEGER || longSide < -IEEE_SAFE_INTEGER) {
            return null;
        }
        return longOnRight
                ? ieeeCompare(doubleSide, (double) longSide)
                : ieeeCompare((double) longSide, doubleSide);
    }

    /**
     * IEEE-754 ordered comparison that agrees with the {@code ==} operator used by
     * {@link #equalsValuesOrNull}. {@link Double#compare} uses Java's total ordering, under which
     * {@code Double.compare(-0.0, +0.0) == -1}; combined with {@code -0.0 == +0.0} that gives an
     * inconsistent comparator: a body value of {@code -0.0} would satisfy BOTH {@code body.x < 0.0}
     * and {@code body.x == 0.0}, breaking the assumption that {@code <}, {@code ==}, {@code >}
     * partition the number line. NaN is already rejected at literal time and at JSON-body parse
     * time, so it cannot reach this method; if it ever did, {@code l < r} and {@code l > r} would
     * both be {@code false} and we would fall through to returning {@code 0}, which is the same
     * result {@link Double#compare} would yield in that edge case under IEEE-equal semantics —
     * still preferable to total-ordering's {@code -0.0 < +0.0}.
     */
    private static int ieeeCompare(double l, double r) {
        if (l < r) return -1;
        if (l > r) return 1;
        return 0;
    }

    private static Boolean matches(int cmp, int target, boolean inclusive) {
        cmp = Integer.signum(cmp);
        if (inclusive && cmp == 0) return Boolean.TRUE;
        return cmp == target;
    }

    private enum Op { ADD, SUB, MUL, DIV, MOD }

    private Object arith(Object l, Object r, Op op) {
        if (!(l instanceof Number) || !(r instanceof Number)) {
            // Type-mismatched arithmetic (string + number, absent operand + number, nested
            // arith-error sentinel + number, ...) returns SKIP rather than null. A null here
            // would feed equalsValuesOrNull's "null operand → FALSE" branch on the way to NEQ,
            // and NEQ would negate FALSE into a confident TRUE — admitting a record whose
            // arithmetic could not be evaluated. SKIP propagates through evalBinary's SKIP guard
            // and through evalLogical with the OR/AND rescue semantics, so a complex predicate
            // can still be saved by a determinate sibling clause without being silently flipped.
            // E.g. `headers['tenant'] + 1 != 'blocked1'` with header tenant='blocked' must drop
            // the record, not admit it.
            return SKIP;
        }
        boolean wantDouble = l instanceof Double || r instanceof Double;
        if (wantDouble) {
            // Operand-level IEEE-safe-integer guard: a Long beyond 2^53 cannot survive promotion
            // to double, so the rounded value would let a follow-up Double-vs-Double equality
            // match a literal. E.g. `body.id / 1.0 == 9007199254740992.0` would match
            // body.id = 9007199254740993L (Long) because (double) 9007199254740993L rounds to
            // 9007199254740992.0. Refuse early when an operand cannot survive that round-trip.
            if (l instanceof Long && unsafeForDouble(((Long) l).longValue())) {
                return SKIP;
            }
            if (r instanceof Long && unsafeForDouble(((Long) r).longValue())) {
                return SKIP;
            }
            Object result = arithDouble(((Number) l).doubleValue(), ((Number) r).doubleValue(), op);
            if (!(result instanceof Double)) {
                return result; // already SKIP / non-finite
            }
            // Result-level precision validation. Even with both operands inside [-2^53, 2^53],
            // ADD/SUB/MUL/DIV can produce a double whose integer neighbours are not all
            // representable: e.g. (2^53) + 1.0 rounds to 2^53.0, and (2^53 - 1) + 2.0 also
            // rounds to 2^53.0. A predicate like `body.x + N <= 2^53.0` would then admit either
            // Long value, bypassing the intended boundary. Validate the double result against
            // an exact arithmetic expansion (BigDecimal) when the result reaches the unsafe
            // integer range; refuse if precision was lost.
            double dr = (Double) result;
            if (dr >= IEEE_SAFE_INTEGER || dr <= -IEEE_SAFE_INTEGER) {
                BigDecimal exact = exactArith((Number) l, (Number) r, op);
                if (exact == null) {
                    // Non-terminating BigDecimal division (e.g. 1/3, or any irrational ratio):
                    // we cannot validate the rounded double against an exact reference. Inside
                    // the precision-loss zone (|dr| >= 2^53) two different exact mathematical
                    // results can round to the same double, so a follow-up equality against
                    // another precision-loss-zone double can match even though the underlying
                    // values are not equal. Example: with body.id = 2^53,
                    //   (body.id / 0.9999999999999999) == (body.id + 2.0)
                    // would match because both sides round to 9007199254740994.0 although
                    // their exact values differ. SKIP, not return: trusting the double here
                    // is unsafe.
                    return SKIP;
                }
                if (new BigDecimal(dr).compareTo(exact) != 0) {
                    return SKIP;
                }
            }
            return result;
        }
        return arithLong(((Number) l).longValue(), ((Number) r).longValue(), op);
    }

    /**
     * Compute the exact mathematical result of a mixed Long/Double (or Double/Double) operation
     * for the result-level precision check above. Returns {@code null} when the exact result
     * cannot be expressed (non-terminating division, divide/mod by zero) — the caller treats
     * that as "cannot validate, accept the double".
     */
    private static BigDecimal exactArith(Number l, Number r, Op op) {
        BigDecimal bdL = toBigDecimal(l);
        BigDecimal bdR = toBigDecimal(r);
        try {
            switch (op) {
                case ADD: return bdL.add(bdR);
                case SUB: return bdL.subtract(bdR);
                case MUL: return bdL.multiply(bdR);
                case DIV:
                    if (bdR.signum() == 0) return null;
                    return bdL.divide(bdR); // exact-only; throws ArithmeticException if non-terminating
                case MOD:
                    if (bdR.signum() == 0) return null;
                    return bdL.remainder(bdR);
                default:
                    return null;
            }
        } catch (ArithmeticException ae) {
            return null;
        }
    }

    private static BigDecimal toBigDecimal(Number n) {
        if (n instanceof Long) return BigDecimal.valueOf(((Long) n).longValue());
        // BigDecimal(double) captures the exact bit-level value of the double.
        return new BigDecimal(((Double) n).doubleValue());
    }

    private static boolean unsafeForDouble(long v) {
        return v > IEEE_SAFE_INTEGER || v < -IEEE_SAFE_INTEGER;
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
                    return SKIP;
                }
                v = a / b;
                break;
            case MOD:
                if (b == 0.0) {
                    return SKIP;
                }
                v = a % b;
                break;
            default:
                return SKIP;
        }
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            return SKIP;
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
                        return SKIP;
                    }
                    // JLS 15.17.2: Long.MIN_VALUE / -1 overflows silently to Long.MIN_VALUE
                    // (Math.multiplyExact for Long.MAX_VALUE+1 would throw, but `/` does not).
                    // Java 17 lacks Math.divideExact(long, long) (added in 18), so trap the
                    // single overflow case explicitly. Without this, a predicate like
                    // `body.priority / -1 < -100` admits a record with body.priority = Long.MIN_VALUE:
                    // the division wraps to Long.MIN_VALUE, which compares <= -100 as true.
                    if (a == Long.MIN_VALUE && b == -1L) {
                        return SKIP;
                    }
                    return a / b;
                case MOD:
                    if (b == 0) {
                        // Returning SKIP (not null) is critical: a null operand reaches
                        // equalsValuesOrNull's "null vs non-null → FALSE" branch, and NEQ then
                        // negates FALSE to a confident TRUE. A predicate like
                        // `body.x % body.y != 0` with body.y == 0 would then admit the record.
                        // SKIP propagates through evalBinary's SKIP guard and the OR/AND rescue.
                        return SKIP;
                    }
                    // Long.MIN_VALUE % -1 is defined by JLS 15.17.3 to return 0 (not overflow),
                    // so no special-case is needed for MOD.
                    return a % b;
                default:
                    return SKIP;
            }
        } catch (ArithmeticException overflow) {
            // long overflow on +/-/* → propagate as SKIP rather than null. null would feed the
            // null-operand → FALSE → NEQ-TRUE bypass described in arith() above; SKIP propagates
            // upward and lets evalLogical rescue when a sibling clause is determinate.
            return SKIP;
        }
    }

    private Object resolvePath(Ast.Path path, RecordContext ctx) {
        switch (path.root) {
            case "offset":
                // Scalar root with an accessor (e.g. `offset.foo`) is a semantic error: there is
                // no sub-field to address. SKIP, not null: null would feed the NEQ-via-null
                // bypass (e.g. `offset.foo != 0` would admit every record). The parser does not
                // enforce scalar-root no-accessor invariants, so this guard is load-bearing.
                if (!path.accessors.isEmpty()) return SKIP;
                return ctx.offset();
            case "partition":
                if (!path.accessors.isEmpty()) return SKIP;
                return (long) ctx.partition(); // promote to long for uniform numeric handling
            case "timestamp":
                if (!path.accessors.isEmpty()) return SKIP;
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
                // Should never reach here — parser rejects unknown roots. SKIP as defense in
                // depth so a parser regression cannot become an admit-anything bypass.
                return SKIP;
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
        // `key` is a scalar; an accessor (e.g. `key.foo`) is a semantic error. SKIP, not null,
        // to avoid the NEQ-via-null bypass.
        if (!path.accessors.isEmpty()) return SKIP;
        Optional<String> decoded = ctx.keyAsString();
        if (decoded.isPresent()) return decoded.get();
        return ctx.rawKey() == null ? null : SKIP;
    }

    /** Same absent-vs-undecodable distinction as {@link #resolveKey} for header accessors. */
    private Object resolveHeader(Ast.Path path, RecordContext ctx) {
        // Header lookup requires exactly one accessor (`headers['name']`). Zero or multiple
        // accessors is a semantic error → SKIP, for the same NEQ-via-null reason.
        if (path.accessors.size() != 1) return SKIP;
        String name = path.accessors.get(0);
        Optional<String> decoded = ctx.header(name);
        if (decoded.isPresent()) return decoded.get();
        return ctx.rawHeader(name) == null ? null : SKIP;
    }
}
