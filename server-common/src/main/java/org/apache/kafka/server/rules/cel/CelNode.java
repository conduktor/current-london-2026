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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import com.google.re2j.Pattern;

/**
 * AST for the CEL-subset interpreter. Each subclass implements
 * {@link #eval(Function)} returning the natural Java value of that node.
 *
 * <p>Null propagates through field/index access so a missing field on a
 * request never throws. Comparisons against null follow CEL semantics:
 * {@code null == null} is true; anything else compared to null is false.
 */
abstract class CelNode {

    abstract Object eval(Function<String, Object> activation);

    static final class Literal extends CelNode {
        final Object value;

        Literal(Object value) {
            this.value = value;
        }

        @Override
        Object eval(Function<String, Object> a) {
            return value;
        }
    }

    static final class Identifier extends CelNode {
        final String name;

        Identifier(String name) {
            this.name = name;
        }

        @Override
        Object eval(Function<String, Object> a) {
            return a.apply(name);
        }
    }

    static final class Field extends CelNode {
        final CelNode receiver;
        final String name;

        Field(CelNode receiver, String name) {
            this.receiver = receiver;
            this.name = name;
        }

        @Override
        Object eval(Function<String, Object> a) {
            Object r = receiver.eval(a);
            if (r == null) {
                return null;
            }
            if (r instanceof Map) {
                return ((Map<?, ?>) r).get(name);
            }
            return null;
        }
    }

    static final class Index extends CelNode {
        final CelNode receiver;
        final CelNode key;

        Index(CelNode receiver, CelNode key) {
            this.receiver = receiver;
            this.key = key;
        }

        @Override
        Object eval(Function<String, Object> a) {
            Object r = receiver.eval(a);
            if (r == null) {
                return null;
            }
            Object k = key.eval(a);
            if (r instanceof List) {
                if (!(k instanceof Number)) {
                    return null;
                }
                int idx = ((Number) k).intValue();
                List<?> list = (List<?>) r;
                if (idx < 0 || idx >= list.size()) {
                    return null;
                }
                return list.get(idx);
            }
            if (r instanceof Map) {
                return ((Map<?, ?>) r).get(k);
            }
            return null;
        }
    }

    static final class MethodCall extends CelNode {
        final CelNode receiver;
        final String method;
        final List<CelNode> args;

        MethodCall(CelNode receiver, String method, List<CelNode> args) {
            this.receiver = receiver;
            this.method = method;
            this.args = args;
        }

        @Override
        Object eval(Function<String, Object> a) {
            Object r = receiver.eval(a);
            String arg0 = args.isEmpty() ? null : stringArg(args.get(0), a);
            // Audit HIGH-2: charge proportional work to the step budget so a
            // single fat string-op cannot escape the per-request budget — and,
            // crucially, so that the same op inside an attacker-iterated
            // comprehension trips the budget at the actual char-work limit
            // rather than only at the comprehension iteration count.
            if (r instanceof String && arg0 != null) {
                int rl = ((String) r).length();
                int al = arg0.length();
                switch (method) {
                    case "startsWith":
                    case "endsWith":
                        // Both algorithms scan at most arg.length() chars
                        // (linear in the needle, the receiver length is only
                        // relevant for the early-out anchor check).
                        CelLimits.bumpSteps(Math.max(1, al));
                        break;
                    case "contains":
                        // JDK String.contains uses naive O(n·m) substring
                        // search. Charge worst-case work so a 16k×16k contains
                        // is caught BEFORE we walk into the JDK implementation.
                        CelLimits.bumpSteps(CelLimits.saturateToInt((long) rl * (long) al));
                        break;
                    default:
                        // fall through to the unknown-method branch below
                }
            }
            switch (method) {
                case "startsWith":
                    return r instanceof String && arg0 != null && ((String) r).startsWith(arg0);
                case "endsWith":
                    return r instanceof String && arg0 != null && ((String) r).endsWith(arg0);
                case "contains":
                    return r instanceof String && arg0 != null && ((String) r).contains(arg0);
                default:
                    // Defense in depth: CelCompiler.parseDotSuffix rejects
                    // unknown method names AND wrong-arity calls at compile
                    // time (audit LOW-1) and lowers .matches(<literal>) to
                    // a RegexMatch node. Reaching this branch means a
                    // parser regression has constructed a MethodCall the
                    // request path cannot evaluate — fail-open on the rule
                    // (RuleEngine catches CelEvaluationException) rather
                    // than crashing the broker thread.
                    throw new CelEvaluationException("unknown method: " + method);
            }
        }

        private static String stringArg(CelNode n, Function<String, Object> a) {
            Object v = n.eval(a);
            if (!(v instanceof String)) {
                throw new CelEvaluationException("expected string argument, got " + typeOf(v));
            }
            return (String) v;
        }
    }

    /**
     * Specialised method-call node for {@code receiver.matches(<literal>)}.
     * The pattern is compiled exactly once at parse time and stored on the
     * node; runtime evaluation only invokes the matcher. This serves three
     * goals at once:
     *
     * <ol>
     *   <li><b>Bad regex caught early.</b> A
     *       {@link com.google.re2j.PatternSyntaxException} on a malformed
     *       literal raises {@link CelCompilationException} at rule load time,
     *       not on the request hot path.</li>
     *   <li><b>Linear-time matching guaranteed.</b> The implementation is
     *       {@code com.google.re2j} (Google's RE2 Java port), not
     *       {@code java.util.regex}. RE2 is worst-case linear in the input
     *       length regardless of pattern shape — a pattern like
     *       {@code (a+)+b} (catastrophic backtracking in the JDK regex
     *       engine on a no-match input) runs in linear time here. Codex/Gemini
     *       final-audit P1#4.</li>
     *   <li><b>ReDoS surface eliminated.</b> Combined with #2, even an
     *       operator who unwittingly writes a JDK-pathological pattern
     *       cannot cause a request thread to stall on attacker-shaped input.
     *       {@link CelLimits#MAX_REGEX_INPUT_LENGTH} remains as defense in
     *       depth — see its javadoc.</li>
     * </ol>
     *
     * <p>The parser only emits this node when the argument is a string
     * literal — dynamic patterns are rejected at compile time. Operators who
     * need pattern variation can compose {@code startsWith} / {@code endsWith}
     * / {@code contains} or supply multiple literal-pattern rules.
     */
    static final class RegexMatch extends CelNode {
        final CelNode receiver;
        final Pattern pattern;
        final String source;

        RegexMatch(CelNode receiver, Pattern pattern, String source) {
            this.receiver = receiver;
            this.pattern = pattern;
            this.source = source;
        }

        @Override
        Object eval(Function<String, Object> a) {
            Object r = receiver.eval(a);
            if (!(r instanceof String)) {
                return false;
            }
            String s = (String) r;
            if (s.length() > CelLimits.MAX_REGEX_INPUT_LENGTH) {
                throw new CelEvaluationException(
                    "matches(): receiver length " + s.length() + " exceeds "
                        + CelLimits.MAX_REGEX_INPUT_LENGTH);
            }
            // Audit HIGH-2: RE2 is worst-case linear in input length. Charge
            // the input length to the step budget so the same matches() call
            // inside a comprehension trips the budget at the real char-work
            // limit rather than only at the comprehension iteration count.
            CelLimits.bumpSteps(Math.max(1, s.length()));
            return pattern.matcher(s).matches();
        }
    }

    static final class Comprehension extends CelNode {
        enum Kind {
            EXISTS, ALL
        }

        final CelNode receiver;
        final Kind kind;
        final String varName;
        final CelNode predicate;

        Comprehension(CelNode receiver, Kind kind, String varName, CelNode predicate) {
            this.receiver = receiver;
            this.kind = kind;
            this.varName = varName;
            this.predicate = predicate;
        }

        @Override
        Object eval(Function<String, Object> a) {
            Object r = receiver.eval(a);
            if (!(r instanceof List)) {
                return kind == Kind.ALL;
            }
            for (Object item : (List<?>) r) {
                // Per-iteration step budget. The motivation is nested
                // comprehensions over attacker-controlled list sizes:
                // request.x.exists(a, request.y.exists(b, ...)) is O(|x|·|y|)
                // and trivially escalates to seconds of CPU on the request
                // thread for any concrete list pair the engine considers
                // "normal". Bump before doing per-element work so a runaway
                // loop is killed at the budget, not after.
                CelLimits.bumpStep();
                Function<String, Object> scoped = name -> name.equals(varName) ? item : a.apply(name);
                Object v = predicate.eval(scoped);
                boolean b = v instanceof Boolean && (Boolean) v;
                if (kind == Kind.EXISTS && b) {
                    return true;
                }
                if (kind == Kind.ALL && !b) {
                    return false;
                }
            }
            return kind == Kind.ALL;
        }
    }

    static final class SizeCall extends CelNode {
        final CelNode arg;

        SizeCall(CelNode arg) {
            this.arg = arg;
        }

        @Override
        Object eval(Function<String, Object> a) {
            Object v = arg.eval(a);
            if (v == null) {
                return 0L;
            }
            if (v instanceof String) {
                return (long) ((String) v).length();
            }
            if (v instanceof List) {
                return (long) ((List<?>) v).size();
            }
            if (v instanceof Map) {
                return (long) ((Map<?, ?>) v).size();
            }
            throw new CelEvaluationException("size() not applicable to " + v.getClass().getSimpleName());
        }
    }

    static final class Not extends CelNode {
        final CelNode inner;

        Not(CelNode inner) {
            this.inner = inner;
        }

        @Override
        Object eval(Function<String, Object> a) {
            Object v = inner.eval(a);
            if (v == null) {
                return true;
            }
            if (v instanceof Boolean) {
                return !(Boolean) v;
            }
            throw new CelEvaluationException("! requires boolean");
        }
    }

    static final class Negate extends CelNode {
        final CelNode inner;

        Negate(CelNode inner) {
            this.inner = inner;
        }

        @Override
        Object eval(Function<String, Object> a) {
            Object v = inner.eval(a);
            if (v instanceof Long) {
                return -(Long) v;
            }
            if (v instanceof Integer) {
                return -(long) (Integer) v;
            }
            throw new CelEvaluationException("unary - requires number");
        }
    }

    static final class And extends CelNode {
        final CelNode left;
        final CelNode right;

        And(CelNode l, CelNode r) {
            this.left = l;
            this.right = r;
        }

        @Override
        Object eval(Function<String, Object> a) {
            Object l = left.eval(a);
            if (!truthy(l)) {
                return false;
            }
            return truthy(right.eval(a));
        }
    }

    static final class Or extends CelNode {
        final CelNode left;
        final CelNode right;

        Or(CelNode l, CelNode r) {
            this.left = l;
            this.right = r;
        }

        @Override
        Object eval(Function<String, Object> a) {
            Object l = left.eval(a);
            if (truthy(l)) {
                return true;
            }
            return truthy(right.eval(a));
        }
    }

    static final class Compare extends CelNode {
        enum Op {
            EQ, NEQ, LT, LE, GT, GE
        }

        final CelNode left;
        final CelNode right;
        final Op op;

        Compare(CelNode l, Op op, CelNode r) {
            this.left = l;
            this.op = op;
            this.right = r;
        }

        @Override
        Object eval(Function<String, Object> a) {
            Object l = left.eval(a);
            Object r = right.eval(a);
            // Audit HIGH-2: String comparisons walk both strings on equal
            // prefixes (compareTo / equals), so a fat string == in an
            // attacker-iterated comprehension can do millions of char compares
            // without touching the iteration budget. Charge the linear cost
            // for string-vs-string comparisons; numeric/boolean compare is
            // O(1) and bounded by AST node count via MAX_NODES.
            if (l instanceof String && r instanceof String) {
                int len = Math.min(((String) l).length(), ((String) r).length());
                CelLimits.bumpSteps(Math.max(1, len));
            }
            if (op == Op.EQ) {
                return valueEquals(l, r);
            }
            if (op == Op.NEQ) {
                return !valueEquals(l, r);
            }
            if (l == null || r == null) {
                return false;
            }
            int c = compareValues(l, r);
            switch (op) {
                case LT: return c < 0;
                case LE: return c <= 0;
                case GT: return c > 0;
                case GE: return c >= 0;
                default: throw new CelEvaluationException("unhandled compare op " + op);
            }
        }
    }

    static final class InList extends CelNode {
        final CelNode value;
        final CelNode list;

        InList(CelNode value, CelNode list) {
            this.value = value;
            this.list = list;
        }

        @Override
        Object eval(Function<String, Object> a) {
            Object v = value.eval(a);
            Object lst = list.eval(a);
            if (!(lst instanceof List)) {
                return false;
            }
            for (Object item : (List<?>) lst) {
                // Same motivation as the comprehension budget: `value in
                // request.giantList` with an attacker-supplied list lets a
                // single boolean operator drive O(N) equality checks on the
                // request thread. Bump per element so the runaway is killed
                // at the budget rather than after.
                CelLimits.bumpStep();
                // Audit HIGH-2: when both sides are strings, valueEquals
                // walks shared prefix chars. Charge that proportional work
                // too — otherwise a list of long strings beats the budget
                // by amortising O(min(|v|,|item|)) char compares per element
                // at only one step apiece. Mirrors Compare.eval's bump.
                if (v instanceof String && item instanceof String) {
                    int len = Math.min(((String) v).length(), ((String) item).length());
                    if (len > 1) {
                        CelLimits.bumpSteps(len - 1);
                    }
                }
                if (valueEquals(v, item)) {
                    return true;
                }
            }
            return false;
        }
    }

    static final class Arith extends CelNode {
        enum Op {
            ADD, SUB, MUL, DIV, MOD
        }

        final CelNode left;
        final CelNode right;
        final Op op;

        Arith(CelNode l, Op op, CelNode r) {
            this.left = l;
            this.op = op;
            this.right = r;
        }

        @Override
        Object eval(Function<String, Object> a) {
            Object l = left.eval(a);
            Object r = right.eval(a);
            if (op == Op.ADD && l instanceof String && r instanceof String) {
                String ls = (String) l;
                String rs = (String) r;
                // Bound the result before allocating it. A doubling-tree
                // {@code (a+a)+(a+a)…} with an activation that resolves
                // {@code a} to a moderately long header/principal string can
                // accumulate many MB of transient String per request before
                // hitting any other safety limit. Capping the result length
                // keeps per-request memory bounded.
                long total = (long) ls.length() + (long) rs.length();
                if (total > CelLimits.MAX_STRING_RESULT_LEN) {
                    throw new CelEvaluationException(
                        "string concatenation result exceeds "
                            + CelLimits.MAX_STRING_RESULT_LEN + " chars");
                }
                // Audit HIGH-2: charge per-char concat cost so the same +
                // inside an iterated comprehension trips the budget at the
                // total chars-copied limit. Numeric arith stays O(1) and is
                // bounded by AST node count via MAX_NODES.
                CelLimits.bumpSteps(CelLimits.saturateToInt(total));
                return ls + rs;
            }
            if (!(l instanceof Number) || !(r instanceof Number)) {
                throw new CelEvaluationException("arithmetic requires numbers");
            }
            return applyArith(((Number) l).longValue(), ((Number) r).longValue());
        }

        private long applyArith(long li, long ri) {
            switch (op) {
                case ADD: return li + ri;
                case SUB: return li - ri;
                case MUL: return li * ri;
                case DIV:
                    if (ri == 0) {
                        throw new CelEvaluationException("divide by zero");
                    }
                    return li / ri;
                case MOD:
                    if (ri == 0) {
                        throw new CelEvaluationException("modulo by zero");
                    }
                    return li % ri;
                default: throw new CelEvaluationException("unhandled arith op " + op);
            }
        }
    }

    static final class ListLiteral extends CelNode {
        final List<CelNode> items;

        ListLiteral(List<CelNode> items) {
            this.items = items;
        }

        @Override
        Object eval(Function<String, Object> a) {
            List<Object> out = new ArrayList<>(items.size());
            for (CelNode n : items) {
                out.add(n.eval(a));
            }
            return out;
        }
    }

    static final class MapLiteral extends CelNode {
        final List<CelNode> keys;
        final List<CelNode> vals;

        MapLiteral(List<CelNode> keys, List<CelNode> vals) {
            this.keys = keys;
            this.vals = vals;
        }

        @Override
        Object eval(Function<String, Object> a) {
            Map<Object, Object> out = new HashMap<>();
            for (int i = 0; i < keys.size(); i++) {
                out.put(keys.get(i).eval(a), vals.get(i).eval(a));
            }
            return out;
        }
    }

    private static boolean truthy(Object v) {
        if (v == null) {
            return false;
        }
        if (v instanceof Boolean) {
            return (Boolean) v;
        }
        throw new CelEvaluationException("expected boolean, got " + typeOf(v));
    }

    private static int compareValues(Object l, Object r) {
        if (l instanceof Number && r instanceof Number) {
            return Long.compare(((Number) l).longValue(), ((Number) r).longValue());
        }
        if (l instanceof String && r instanceof String) {
            return ((String) l).compareTo((String) r);
        }
        throw new CelEvaluationException(
            "incomparable values: " + typeOf(l) + " vs " + typeOf(r));
    }

    /**
     * Type-only descriptor for an activation value, suitable for inclusion in
     * {@link CelEvaluationException} messages. The exception's {@code toString}
     * is logged at WARN by {@code RuleEngine.evaluate} when a rule fails open,
     * so the message MUST NOT reproduce the runtime value — activation values
     * can carry request-derived data (header values, principal names, topic
     * names) which an operator scanning broker logs should not see by
     * accident. We surface the Java class name only, which is enough to
     * diagnose a misauthored rule (`expected string, got Long`) without
     * leaking the offending payload.
     */
    private static String typeOf(Object v) {
        return v == null ? "null" : v.getClass().getSimpleName();
    }

    /**
     * Equality with numeric promotion: Integer(5) equals Long(5). Anything else
     * defers to {@link Objects#equals}. CEL treats all integers as the same value
     * type; Java's autoboxing produces distinct wrappers we must reconcile.
     */
    private static boolean valueEquals(Object l, Object r) {
        if (l instanceof Number && r instanceof Number) {
            return ((Number) l).longValue() == ((Number) r).longValue();
        }
        return Objects.equals(l, r);
    }
}
