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
import java.util.regex.Pattern;

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
            switch (method) {
                case "startsWith":
                    return r instanceof String && arg0 != null && ((String) r).startsWith(arg0);
                case "endsWith":
                    return r instanceof String && arg0 != null && ((String) r).endsWith(arg0);
                case "contains":
                    return r instanceof String && arg0 != null && ((String) r).contains(arg0);
                default:
                    // matches() is not handled here — the parser lowers
                    // `.matches(<literal>)` into a RegexMatch node so the
                    // pattern is pre-compiled at rule load time. Reaching
                    // this branch means the parser failed to enforce that
                    // contract.
                    throw new CelEvaluationException("unknown method: " + method);
            }
        }

        private static String stringArg(CelNode n, Function<String, Object> a) {
            Object v = n.eval(a);
            if (!(v instanceof String)) {
                throw new CelEvaluationException("expected string argument, got " + v);
            }
            return (String) v;
        }
    }

    /**
     * Specialised method-call node for {@code receiver.matches(<literal>)}.
     * The pattern is compiled exactly once at parse time and stored on the
     * node; runtime evaluation only invokes the matcher. This serves two
     * goals at once:
     *
     * <ol>
     *   <li><b>Bad regex caught early.</b> A {@link java.util.regex.PatternSyntaxException}
     *       on a malformed literal raises {@link CelCompilationException} at
     *       rule load time, not on the request hot path.</li>
     *   <li><b>ReDoS surface reduced.</b> The receiver string is bounded by
     *       {@link CelLimits#MAX_REGEX_INPUT_LENGTH}; longer inputs raise
     *       {@link CelEvaluationException} (which the engine fails open on),
     *       so a request crafted to feed a megabyte-long field to a
     *       backtracking regex cannot stall the request thread.</li>
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
                return l + (String) r;
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
        throw new CelEvaluationException("expected boolean, got " + v);
    }

    private static int compareValues(Object l, Object r) {
        if (l instanceof Number && r instanceof Number) {
            return Long.compare(((Number) l).longValue(), ((Number) r).longValue());
        }
        if (l instanceof String && r instanceof String) {
            return ((String) l).compareTo((String) r);
        }
        throw new CelEvaluationException("incomparable values: " + l + " vs " + r);
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
