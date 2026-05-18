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
        try {
            Parser p = new Parser(new Lexer(source).tokenize());
            CelNode root = p.parseExpr();
            p.expectEof();
            return new CelProgram(root, source);
        } catch (CelCompilationException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new CelCompilationException("failed to parse: " + source + " (" + e.getMessage() + ")", e);
        }
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
                if (Character.isDigit(c)) {
                    out.add(number(start));
                } else if (c == '"' || c == '\'') {
                    out.add(string(start, c));
                } else if (Character.isLetter(c) || c == '_') {
                    out.add(identOrKeyword(start));
                } else {
                    out.add(symbolOrFail(start, c));
                }
            }
            out.add(new Token(TokKind.EOF, "", null, i));
            return out;
        }

        private Token number(int start) {
            while (i < src.length() && Character.isDigit(src.charAt(i))) {
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
            while (i < src.length() && (Character.isLetterOrDigit(src.charAt(i)) || src.charAt(i) == '_')) {
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

        Parser(List<Token> toks) {
            this.toks = toks;
            this.i = 0;
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

        CelNode parseExpr() {
            return parseOr();
        }

        private CelNode parseOr() {
            CelNode left = parseAnd();
            while (match(TokKind.OR)) {
                left = new CelNode.Or(left, parseAnd());
            }
            return left;
        }

        private CelNode parseAnd() {
            CelNode left = parseNot();
            while (match(TokKind.AND)) {
                left = new CelNode.And(left, parseNot());
            }
            return left;
        }

        private CelNode parseNot() {
            if (match(TokKind.NOT)) {
                return new CelNode.Not(parseNot());
            }
            return parseRel();
        }

        private CelNode parseRel() {
            CelNode left = parseAdd();
            CelNode.Compare.Op op = COMPARE_OPS.get(peek().kind);
            if (op != null) {
                consume();
                return new CelNode.Compare(left, op, parseAdd());
            }
            if (match(TokKind.IN)) {
                return new CelNode.InList(left, parseAdd());
            }
            return left;
        }

        private CelNode parseAdd() {
            CelNode left = parseMul();
            while (true) {
                if (match(TokKind.PLUS)) {
                    left = new CelNode.Arith(left, CelNode.Arith.Op.ADD, parseMul());
                } else if (match(TokKind.MINUS)) {
                    left = new CelNode.Arith(left, CelNode.Arith.Op.SUB, parseMul());
                } else {
                    return left;
                }
            }
        }

        private CelNode parseMul() {
            CelNode left = parseUnary();
            while (true) {
                if (match(TokKind.STAR)) {
                    left = new CelNode.Arith(left, CelNode.Arith.Op.MUL, parseUnary());
                } else if (match(TokKind.SLASH)) {
                    left = new CelNode.Arith(left, CelNode.Arith.Op.DIV, parseUnary());
                } else if (match(TokKind.PERCENT)) {
                    left = new CelNode.Arith(left, CelNode.Arith.Op.MOD, parseUnary());
                } else {
                    return left;
                }
            }
        }

        private CelNode parseUnary() {
            if (match(TokKind.MINUS)) {
                return new CelNode.Negate(parseUnary());
            }
            return parsePostfix();
        }

        private CelNode parsePostfix() {
            CelNode node = parsePrimary();
            while (true) {
                if (match(TokKind.DOT)) {
                    node = parseDotSuffix(node);
                } else if (match(TokKind.LBRACK)) {
                    CelNode idx = parseExpr();
                    expect(TokKind.RBRACK);
                    node = new CelNode.Index(node, idx);
                } else {
                    return node;
                }
            }
        }

        private CelNode parseDotSuffix(CelNode receiver) {
            Token name = expect(TokKind.IDENT);
            if (peek().kind != TokKind.LPAREN) {
                return new CelNode.Field(receiver, name.text);
            }
            consume();
            if ("exists".equals(name.text) || "all".equals(name.text)) {
                return parseComprehension(receiver, name.text);
            }
            return new CelNode.MethodCall(receiver, name.text, parseArgList());
        }

        private CelNode parseComprehension(CelNode receiver, String macroName) {
            Token var = expect(TokKind.IDENT);
            expect(TokKind.COMMA);
            CelNode pred = parseExpr();
            expect(TokKind.RPAREN);
            CelNode.Comprehension.Kind kind = "exists".equals(macroName)
                ? CelNode.Comprehension.Kind.EXISTS
                : CelNode.Comprehension.Kind.ALL;
            return new CelNode.Comprehension(receiver, kind, var.text, pred);
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
                    return new CelNode.Literal(t.literal);
                case TRUE:
                    consume();
                    return new CelNode.Literal(Boolean.TRUE);
                case FALSE:
                    consume();
                    return new CelNode.Literal(Boolean.FALSE);
                case NULL:
                    consume();
                    return new CelNode.Literal(null);
                default:
                    return null;
            }
        }

        private CelNode parseIdentOrCall() {
            Token t = consume();
            if (peek().kind != TokKind.LPAREN) {
                return new CelNode.Identifier(t.text);
            }
            consume();
            List<CelNode> args = parseArgList();
            if ("size".equals(t.text) && args.size() == 1) {
                return new CelNode.SizeCall(args.get(0));
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
            return new CelNode.ListLiteral(items);
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
            return new CelNode.MapLiteral(keys, vals);
        }

        private void parseMapEntry(List<CelNode> keys, List<CelNode> vals) {
            keys.add(parseExpr());
            expect(TokKind.COLON);
            vals.add(parseExpr());
        }
    }
}
