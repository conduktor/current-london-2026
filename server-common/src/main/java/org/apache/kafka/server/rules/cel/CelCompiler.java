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

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;

/**
 * Lexer + recursive-descent parser for the CEL subset supported by the
 * broker rule engine. Compile once at rule load; reuse {@link CelProgram}
 * on every evaluation.
 */
public final class CelCompiler {

    private CelCompiler() {
    }

    public static CelProgram compile(String source) {
        if (source == null || source.trim().isEmpty()) {
            throw new CelCompilationException("empty expression");
        }
        // Codex final-audit P1#5: reject pathologically long sources BEFORE
        // tokenising. The lexer allocates one Token per character span; a
        // multi-megabyte source — typically a giant string or regex literal —
        // would otherwise pin tens of megabytes of token objects in the
        // rule-load thread before MAX_NODES kicks in. See CelLimits.MAX_EXPR_LEN.
        if (source.length() > CelLimits.MAX_EXPR_LEN) {
            throw new CelCompilationException(
                "CEL source length " + source.length()
                    + " exceeds MAX_EXPR_LEN of " + CelLimits.MAX_EXPR_LEN
                    + "; rules approaching this cap are almost certainly "
                    + "smuggling a giant string or regex literal — rewrite as "
                    + "a coarser filter or split into multiple rules");
        }
        try {
            Parser p = new Parser(new Lexer(source).tokenize());
            CelNode root = p.parseExpr();
            p.expectEof();
            return new CelProgram(root, source);
        } catch (CelCompilationException e) {
            throw e;
        } catch (RuntimeException e) {
            // Round-11 audit (audit-forgery sub-agent, CRITICAL): the source
            // is bounded by MAX_EXPR_LEN=8192, which means a deliberately
            // malformed envelope can pin an 8 KB string into every parse-
            // failure log line emitted by GovernanceLoader. Truncate to a
            // short head so the message stays diagnostically useful (the
            // operator can usually identify the rule from the first ~80
            // chars) without amplifying every rejected envelope. The full
            // source is still available on the rule envelope itself — this
            // only bounds the log-line side-channel.
            //
            // The cause's own getMessage() also echoes the offending input
            // for some JDK exceptions (e.g. NumberFormatException from
            // Long.parseLong embeds the raw digit string). Truncate both
            // sides so neither contributes an 8 KB tail.
            throw new CelCompilationException(
                "failed to parse: " + truncateForLog(source)
                    + " (" + truncateForLog(e.getMessage()) + ")", e);
        }
    }

    /**
     * Truncate a CEL source string for embedding in an exception/log message.
     * Operator-authored rules under ~80 chars pass through unchanged; longer
     * ones get a head fragment plus an ellipsis + length annotation so the
     * rejection log line stays bounded regardless of {@link CelLimits#MAX_EXPR_LEN}.
     */
    static String truncateForLog(String source) {
        if (source == null) {
            return "null";
        }
        int max = 80;
        if (source.length() <= max) {
            return source;
        }
        return source.substring(0, max) + "...[truncated, " + source.length() + " chars]";
    }

    enum TokKind {
        NUM, STR, IDENT, TRUE, FALSE, NULL,
        DOT, COMMA, LPAREN, RPAREN, LBRACK, RBRACK, LBRACE, RBRACE, COLON,
        PLUS, MINUS, STAR, SLASH, PERCENT,
        EQ, NEQ, LT, LE, GT, GE,
        AND, OR, NOT, IN,
        EOF
    }

    static final class Token {
        final TokKind kind;
        final String text;
        final Object literal;
        final int pos;

        Token(TokKind kind, String text, Object literal, int pos) {
            this.kind = kind;
            this.text = text;
            this.literal = literal;
            this.pos = pos;
        }

        @Override
        public String toString() {
            return kind + "(" + text + ")";
        }
    }

    static final class Lexer {
        private static final Map<String, TokKind> KEYWORDS = new HashMap<>();
        static {
            KEYWORDS.put("true", TokKind.TRUE);
            KEYWORDS.put("false", TokKind.FALSE);
            KEYWORDS.put("null", TokKind.NULL);
            KEYWORDS.put("in", TokKind.IN);
        }

        private final String src;
        private int i;

        Lexer(String src) {
            this.src = src;
            this.i = 0;
        }

        List<Token> tokenize() {
            List<Token> out = new ArrayList<>();
            while (i < src.length()) {
                char c = src.charAt(i);
                if (Character.isWhitespace(c)) {
                    i++;
                    continue;
                }
                int start = i;
                // R28 adversarial (#247): identifier/numeric tokens are ASCII
                // only. Character.isDigit and Character.isLetter admit ~700
                // Unicode digit codepoints (Arabic-Indic, Devanagari, fullwidth)
                // and thousands of letter codepoints (Cyrillic, Greek, CJK
                // homoglyphs). Long.parseLong below would then reject the
                // non-ASCII numerics with a NumberFormatException whose message
                // embeds the raw input — wasted compile work and a misleading
                // diagnostic. For identifiers, a Cyrillic 'р' (U+0440) would
                // tokenise as IDENT but the activation supplier never resolves
                // the non-ASCII name, so the rule silently never fires — a
                // homoglyph trap for a reviewer reading the rule JSON. The
                // CEL grammar is ASCII; pin it.
                if (isAsciiDigit(c)) {
                    out.add(number(start));
                } else if (c == '"' || c == '\'') {
                    out.add(string(start, c));
                } else if (isAsciiLetter(c) || c == '_') {
                    out.add(identOrKeyword(start));
                } else {
                    out.add(symbolOrFail(start, c));
                }
            }
            out.add(new Token(TokKind.EOF, "", null, i));
            return out;
        }

        private static boolean isAsciiDigit(char c) {
            return c >= '0' && c <= '9';
        }

        private static boolean isAsciiLetter(char c) {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
        }

        private static boolean isAsciiLetterOrDigit(char c) {
            return isAsciiDigit(c) || isAsciiLetter(c);
        }

        private Token number(int start) {
            while (i < src.length() && isAsciiDigit(src.charAt(i))) {
                i++;
            }
            long value = Long.parseLong(src.substring(start, i));
            return new Token(TokKind.NUM, src.substring(start, i), value, start);
        }

        private Token string(int start, char quote) {
            i++;
            StringBuilder sb = new StringBuilder();
            while (i < src.length() && src.charAt(i) != quote) {
                char c = src.charAt(i);
                if (c == '\\' && i + 1 < src.length()) {
                    appendEscaped(sb, src.charAt(i + 1));
                    i += 2;
                } else {
                    sb.append(c);
                    i++;
                }
            }
            if (i >= src.length()) {
                throw new CelCompilationException("unterminated string at " + start);
            }
            i++;
            return new Token(TokKind.STR, src.substring(start, i), sb.toString(), start);
        }

        private void appendEscaped(StringBuilder sb, char next) {
            switch (next) {
                case 'n':
                    sb.append('\n');
                    break;
                case 't':
                    sb.append('\t');
                    break;
                case 'r':
                    sb.append('\r');
                    break;
                case '\\':
                    sb.append('\\');
                    break;
                case '"':
                    sb.append('"');
                    break;
                case '\'':
                    sb.append('\'');
                    break;
                default:
                    sb.append(next);
                    break;
            }
        }

        private Token identOrKeyword(int start) {
            while (i < src.length() && (isAsciiLetterOrDigit(src.charAt(i)) || src.charAt(i) == '_')) {
                i++;
            }
            String name = src.substring(start, i);
            TokKind keyword = KEYWORDS.get(name);
            if (keyword == TokKind.TRUE) {
                return new Token(keyword, name, Boolean.TRUE, start);
            }
            if (keyword == TokKind.FALSE) {
                return new Token(keyword, name, Boolean.FALSE, start);
            }
            if (keyword != null) {
                return new Token(keyword, name, null, start);
            }
            return new Token(TokKind.IDENT, name, name, start);
        }

        private Token symbolOrFail(int start, char c) {
            Token t = symbol(start, c);
            if (t == null) {
                throw new CelCompilationException("unexpected character '" + c + "' at " + i);
            }
            return t;
        }

        private Token symbol(int start, char c) {
            char next = (i + 1 < src.length()) ? src.charAt(i + 1) : '\0';
            Token single = singleCharSymbol(start, c);
            if (single != null) {
                return single;
            }
            return multiCharSymbol(start, c, next);
        }

        private Token singleCharSymbol(int start, char c) {
            TokKind kind = SINGLE_CHAR.get(c);
            if (kind == null) {
                return null;
            }
            i++;
            return new Token(kind, String.valueOf(c), null, start);
        }

        private Token multiCharSymbol(int start, char c, char next) {
            switch (c) {
                case '!': return next == '=' ? consume2(TokKind.NEQ, "!=", start) : consume1(TokKind.NOT, "!", start);
                case '=': return next == '=' ? consume2(TokKind.EQ, "==", start) : null;
                case '<': return next == '=' ? consume2(TokKind.LE, "<=", start) : consume1(TokKind.LT, "<", start);
                case '>': return next == '=' ? consume2(TokKind.GE, ">=", start) : consume1(TokKind.GT, ">", start);
                case '&': return next == '&' ? consume2(TokKind.AND, "&&", start) : null;
                case '|': return next == '|' ? consume2(TokKind.OR, "||", start) : null;
                default: return null;
            }
        }

        private Token consume1(TokKind kind, String text, int start) {
            i++;
            return new Token(kind, text, null, start);
        }

        private Token consume2(TokKind kind, String text, int start) {
            i += 2;
            return new Token(kind, text, null, start);
        }

        private static final Map<Character, TokKind> SINGLE_CHAR = new HashMap<>();
        static {
            SINGLE_CHAR.put('.', TokKind.DOT);
            SINGLE_CHAR.put(',', TokKind.COMMA);
            SINGLE_CHAR.put('(', TokKind.LPAREN);
            SINGLE_CHAR.put(')', TokKind.RPAREN);
            SINGLE_CHAR.put('[', TokKind.LBRACK);
            SINGLE_CHAR.put(']', TokKind.RBRACK);
            SINGLE_CHAR.put('{', TokKind.LBRACE);
            SINGLE_CHAR.put('}', TokKind.RBRACE);
            SINGLE_CHAR.put(':', TokKind.COLON);
            SINGLE_CHAR.put('+', TokKind.PLUS);
            SINGLE_CHAR.put('-', TokKind.MINUS);
            SINGLE_CHAR.put('*', TokKind.STAR);
            SINGLE_CHAR.put('/', TokKind.SLASH);
            SINGLE_CHAR.put('%', TokKind.PERCENT);
        }
    }

    static final class Parser {
        private static final Map<TokKind, CelNode.Compare.Op> COMPARE_OPS = new HashMap<>();
        static {
            COMPARE_OPS.put(TokKind.EQ, CelNode.Compare.Op.EQ);
            COMPARE_OPS.put(TokKind.NEQ, CelNode.Compare.Op.NEQ);
            COMPARE_OPS.put(TokKind.LT, CelNode.Compare.Op.LT);
            COMPARE_OPS.put(TokKind.LE, CelNode.Compare.Op.LE);
            COMPARE_OPS.put(TokKind.GT, CelNode.Compare.Op.GT);
            COMPARE_OPS.put(TokKind.GE, CelNode.Compare.Op.GE);
        }

        private final List<Token> toks;
        private int i;
        private int depth;
        private int nodes;

        Parser(List<Token> toks) {
            this.toks = toks;
            this.i = 0;
            this.depth = 0;
            this.nodes = 0;
        }

        void expectEof() {
            if (peek().kind != TokKind.EOF) {
                throw new CelCompilationException("unexpected token after expression: " + peek());
            }
        }

        Token peek() {
            return toks.get(i);
        }

        Token consume() {
            return toks.get(i++);
        }

        boolean match(TokKind k) {
            if (peek().kind == k) {
                i++;
                return true;
            }
            return false;
        }

        Token expect(TokKind k) {
            if (peek().kind != k) {
                throw new CelCompilationException("expected " + k + ", got " + peek());
            }
            return consume();
        }

        /**
         * Account for a freshly-allocated node. Every {@code new CelNode.X(...)}
         * inside the parser is wrapped in {@code account(...)} so the parser
         * enforces {@link CelLimits#MAX_NODES} regardless of which production
         * is allocating. Crossing the limit raises {@link CelCompilationException}
         * with the offending source — the operator sees the error at rule
         * load, not in production.
         */
        private <T extends CelNode> T account(T node) {
            nodes++;
            if (nodes > CelLimits.MAX_NODES) {
                throw new CelCompilationException(
                    "CEL expression exceeds node budget of " + CelLimits.MAX_NODES);
            }
            return node;
        }

        /**
         * Pre-increment the parse-call depth counter and throw if it would
         * exceed {@link CelLimits#MAX_PARSE_DEPTH}. Callers MUST pair this
         * with {@link #exitDepth()} in a try/finally so an exception during
         * sub-parsing does not leave the counter elevated for the next call
         * on the same parser instance. (In practice the parser is one-shot
         * per source, but defensive symmetry is cheap.)
         */
        private void enterDepth() {
            depth++;
            if (depth > CelLimits.MAX_PARSE_DEPTH) {
                throw new CelCompilationException(
                    "CEL expression exceeds parse depth budget of " + CelLimits.MAX_PARSE_DEPTH);
            }
        }

        private void exitDepth() {
            depth--;
        }

        CelNode parseExpr() {
            enterDepth();
            try {
                return parseOr();
            } finally {
                exitDepth();
            }
        }

        private CelNode parseOr() {
            CelNode left = parseAnd();
            while (match(TokKind.OR)) {
                left = account(new CelNode.Or(left, parseAnd()));
            }
            return left;
        }

        private CelNode parseAnd() {
            CelNode left = parseNot();
            while (match(TokKind.AND)) {
                left = account(new CelNode.And(left, parseNot()));
            }
            return left;
        }

        private CelNode parseNot() {
            // parseNot recurses into itself for every leading `!`, so an input
            // like "!!!!...x" can blow the JVM stack at parse time. Bound it.
            enterDepth();
            try {
                if (match(TokKind.NOT)) {
                    return account(new CelNode.Not(parseNot()));
                }
                return parseRel();
            } finally {
                exitDepth();
            }
        }

        private CelNode parseRel() {
            CelNode left = parseAdd();
            CelNode.Compare.Op op = COMPARE_OPS.get(peek().kind);
            if (op != null) {
                consume();
                return account(new CelNode.Compare(left, op, parseAdd()));
            }
            if (match(TokKind.IN)) {
                return account(new CelNode.InList(left, parseAdd()));
            }
            return left;
        }

        private CelNode parseAdd() {
            CelNode left = parseMul();
            while (true) {
                if (match(TokKind.PLUS)) {
                    left = account(new CelNode.Arith(left, CelNode.Arith.Op.ADD, parseMul()));
                } else if (match(TokKind.MINUS)) {
                    left = account(new CelNode.Arith(left, CelNode.Arith.Op.SUB, parseMul()));
                } else {
                    return left;
                }
            }
        }

        private CelNode parseMul() {
            CelNode left = parseUnary();
            while (true) {
                if (match(TokKind.STAR)) {
                    left = account(new CelNode.Arith(left, CelNode.Arith.Op.MUL, parseUnary()));
                } else if (match(TokKind.SLASH)) {
                    left = account(new CelNode.Arith(left, CelNode.Arith.Op.DIV, parseUnary()));
                } else if (match(TokKind.PERCENT)) {
                    left = account(new CelNode.Arith(left, CelNode.Arith.Op.MOD, parseUnary()));
                } else {
                    return left;
                }
            }
        }

        private CelNode parseUnary() {
            // Same rationale as parseNot — unbounded leading `-` would
            // overflow the parse stack.
            enterDepth();
            try {
                if (match(TokKind.MINUS)) {
                    return account(new CelNode.Negate(parseUnary()));
                }
                return parsePostfix();
            } finally {
                exitDepth();
            }
        }

        private CelNode parsePostfix() {
            CelNode node = parsePrimary();
            while (true) {
                if (match(TokKind.DOT)) {
                    node = parseDotSuffix(node);
                } else if (match(TokKind.LBRACK)) {
                    CelNode idx = parseExpr();
                    expect(TokKind.RBRACK);
                    node = account(new CelNode.Index(node, idx));
                } else {
                    return node;
                }
            }
        }

        private CelNode parseDotSuffix(CelNode receiver) {
            Token name = expect(TokKind.IDENT);
            if (peek().kind != TokKind.LPAREN) {
                return account(new CelNode.Field(receiver, name.text));
            }
            consume();
            if ("exists".equals(name.text) || "all".equals(name.text)) {
                return parseComprehension(receiver, name.text);
            }
            List<CelNode> args = parseArgList();
            if ("matches".equals(name.text)) {
                return buildRegexMatch(receiver, args);
            }
            // Audit LOW-1: reject unknown method names at compile time, not
            // eval time. The pre-fix behaviour built a MethodCall with any
            // identifier and threw CelEvaluationException only when the
            // request path tried to evaluate it — at which point RuleEngine
            // fails the rule open (logged and skipped). That is a deferred
            // and silent failure mode: a typo like `name.startWith("audit")`
            // (missing 's') would install successfully, never fire, and
            // produce a misleading "this rule does nothing" diagnostic only
            // by reading the broker logs. Catching it here at rule-load
            // turns the typo into a CelCompilationException → the JSON
            // codec rejects the envelope → GovernanceLoader returns false →
            // the previously-good RuleSet is preserved. The rule submitter
            // gets a precise, immediate diagnostic instead of silence.
            //
            // Arg-count is also a load-time concern: `s.startsWith()` and
            // `s.contains(a, b)` are programming errors, not request-shape
            // surprises, and they should be caught before any request walks
            // an obviously-wrong rule. Each of our string methods takes
            // exactly one argument.
            if (!"startsWith".equals(name.text)
                    && !"endsWith".equals(name.text)
                    && !"contains".equals(name.text)) {
                throw new CelCompilationException(
                    "unknown method '" + name.text + "()' on receiver; "
                        + "supported methods: startsWith, endsWith, contains, matches, exists, all");
            }
            if (args.size() != 1) {
                throw new CelCompilationException(
                    name.text + "() requires exactly one argument, got " + args.size());
            }
            return account(new CelNode.MethodCall(receiver, name.text, args));
        }

        /**
         * Lower {@code receiver.matches(<literal>)} to a {@link CelNode.RegexMatch}
         * with a pre-compiled {@link Pattern}. We require exactly one string
         * literal argument: a dynamic pattern would have to be compiled per
         * request, which both costs CPU and defeats compile-time validation
         * of the regex syntax. Operators who want pattern variation should
         * supply multiple literal-pattern rules.
         *
         * <p>The compile happens here, at rule load. A malformed regex
         * surfaces as {@link CelCompilationException} to the rule submitter,
         * not as a runtime exception on the broker request path.
         *
         * <p>The compiler is {@code com.google.re2j} (the Java port of Google's
         * RE2), not {@code java.util.regex}. RE2 guarantees worst-case linear
         * time in the input length regardless of pattern shape; it is
         * impossible to write a CEL regex that exhibits catastrophic
         * backtracking on a hostile receiver. The trade-off is that RE2 does
         * not support backreferences ({@code \1}), lookaround
         * ({@code (?=...)} / {@code (?!...)} / {@code (?<=...)} /
         * {@code (?<!...)}), or possessive quantifiers ({@code a*+},
         * {@code a++}). A rule that needs any of those should be rewritten as
         * multiple simpler rules; the linear-time guarantee on the request
         * thread is the more valuable property. Codex/Gemini final-audit P1#4.
         */
        private CelNode buildRegexMatch(CelNode receiver, List<CelNode> args) {
            if (args.size() != 1) {
                throw new CelCompilationException(
                    "matches() requires exactly one argument, got " + args.size());
            }
            CelNode argNode = args.get(0);
            if (!(argNode instanceof CelNode.Literal)) {
                throw new CelCompilationException(
                    "matches() requires a string literal pattern; dynamic patterns "
                        + "are not supported because they cannot be pre-compiled");
            }
            Object literal = ((CelNode.Literal) argNode).value;
            if (!(literal instanceof String)) {
                throw new CelCompilationException(
                    "matches() requires a string literal pattern, got " + literal);
            }
            String patternSrc = (String) literal;
            try {
                Pattern compiled = Pattern.compile(patternSrc);
                return account(new CelNode.RegexMatch(receiver, compiled, patternSrc));
            } catch (PatternSyntaxException e) {
                throw new CelCompilationException("invalid regex: " + patternSrc, e);
            }
        }

        private CelNode parseComprehension(CelNode receiver, String macroName) {
            Token var = expect(TokKind.IDENT);
            expect(TokKind.COMMA);
            CelNode pred = parseExpr();
            expect(TokKind.RPAREN);
            CelNode.Comprehension.Kind kind = "exists".equals(macroName)
                ? CelNode.Comprehension.Kind.EXISTS
                : CelNode.Comprehension.Kind.ALL;
            return account(new CelNode.Comprehension(receiver, kind, var.text, pred));
        }

        private List<CelNode> parseArgList() {
            List<CelNode> out = new ArrayList<>();
            if (peek().kind == TokKind.RPAREN) {
                consume();
                return out;
            }
            out.add(parseExpr());
            while (match(TokKind.COMMA)) {
                out.add(parseExpr());
            }
            expect(TokKind.RPAREN);
            return out;
        }

        private CelNode parsePrimary() {
            Token t = peek();
            CelNode literalNode = tryParseLiteral(t);
            if (literalNode != null) {
                return literalNode;
            }
            switch (t.kind) {
                case IDENT: return parseIdentOrCall();
                case LPAREN: return parseGrouped();
                case LBRACK: return parseListLiteral();
                case LBRACE: return parseMapLiteral();
                default: throw new CelCompilationException("unexpected token: " + t);
            }
        }

        private CelNode tryParseLiteral(Token t) {
            switch (t.kind) {
                case NUM:
                case STR:
                    consume();
                    return account(new CelNode.Literal(t.literal));
                case TRUE:
                    consume();
                    return account(new CelNode.Literal(Boolean.TRUE));
                case FALSE:
                    consume();
                    return account(new CelNode.Literal(Boolean.FALSE));
                case NULL:
                    consume();
                    return account(new CelNode.Literal(null));
                default:
                    return null;
            }
        }

        private CelNode parseIdentOrCall() {
            Token t = consume();
            if (peek().kind != TokKind.LPAREN) {
                return account(new CelNode.Identifier(t.text));
            }
            consume();
            List<CelNode> args = parseArgList();
            if ("size".equals(t.text) && args.size() == 1) {
                return account(new CelNode.SizeCall(args.get(0)));
            }
            throw new CelCompilationException("unknown function: " + t.text);
        }

        private CelNode parseGrouped() {
            consume();
            CelNode inner = parseExpr();
            expect(TokKind.RPAREN);
            return inner;
        }

        private CelNode parseListLiteral() {
            consume();
            List<CelNode> items = new ArrayList<>();
            if (peek().kind != TokKind.RBRACK) {
                items.add(parseExpr());
                while (match(TokKind.COMMA)) {
                    items.add(parseExpr());
                }
            }
            expect(TokKind.RBRACK);
            return account(new CelNode.ListLiteral(items));
        }

        private CelNode parseMapLiteral() {
            consume();
            List<CelNode> keys = new ArrayList<>();
            List<CelNode> vals = new ArrayList<>();
            if (peek().kind != TokKind.RBRACE) {
                parseMapEntry(keys, vals);
                while (match(TokKind.COMMA)) {
                    parseMapEntry(keys, vals);
                }
            }
            expect(TokKind.RBRACE);
            return account(new CelNode.MapLiteral(keys, vals));
        }

        private void parseMapEntry(List<CelNode> keys, List<CelNode> vals) {
            keys.add(parseExpr());
            expect(TokKind.COLON);
            vals.add(parseExpr());
        }
    }
}
