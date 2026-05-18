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
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * Hand-written lexer for the view predicate language. Emits a flat list of tokens that the parser
 * consumes. String literal length is capped per {@link PredicateLimits#maxStringLiteralLength};
 * integer literals beyond Long range are rejected here.
 *
 * The token kinds intentionally exclude any keyword/symbol that would enable function calls,
 * comprehensions, or regex — the grammar itself can't form them.
 */
final class Lexer {
    enum Kind {
        // literals
        INT_LITERAL, FLOAT_LITERAL, STRING_LITERAL, TRUE, FALSE, NULL,
        // identifiers
        IDENTIFIER,
        // operators
        AND, OR, NOT,
        EQ, NEQ, LT, LTE, GT, GTE,
        PLUS, MINUS, STAR, SLASH, PERCENT,
        // punctuation
        DOT, LBRACKET, RBRACKET, LPAREN, RPAREN,
        EOF
    }

    static final class Token {
        final Kind kind;
        final String text;
        final Object value; // Long, Double, or String for literals; null otherwise
        final int pos;
        Token(Kind kind, String text, Object value, int pos) {
            this.kind = kind;
            this.text = text;
            this.value = value;
            this.pos = pos;
        }
        @Override public String toString() {
            return kind + "(" + text + ")";
        }
    }

    private final String src;
    private final PredicateLimits limits;
    private int i = 0;

    Lexer(String src, PredicateLimits limits) {
        this.src = src;
        this.limits = limits;
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
            if (Character.isLetter(c) || c == '_') {
                out.add(readIdentifierOrKeyword(start));
            } else if (Character.isDigit(c)) {
                out.add(readNumber(start));
            } else if (c == '\'' || c == '"') {
                out.add(readString(start, c));
            } else {
                out.add(readSymbol(start));
            }
        }
        out.add(new Token(Kind.EOF, "", null, src.length()));
        return out;
    }

    private Token readIdentifierOrKeyword(int start) {
        while (i < src.length()) {
            char c = src.charAt(i);
            if (Character.isLetterOrDigit(c) || c == '_') {
                i++;
            } else {
                break;
            }
        }
        String text = src.substring(start, i);
        switch (text) {
            case "true":
                return new Token(Kind.TRUE, text, Boolean.TRUE, start);
            case "false":
                return new Token(Kind.FALSE, text, Boolean.FALSE, start);
            case "null":
                return new Token(Kind.NULL, text, null, start);
            default:
                return new Token(Kind.IDENTIFIER, text, text, start);
        }
    }

    private Token readNumber(int start) {
        boolean isFloat = scanIntegerDigits();
        isFloat = scanFractionalPart() || isFloat;
        isFloat = scanExponentPart() || isFloat;
        String text = src.substring(start, i);
        return isFloat ? buildFloatToken(text, start) : buildIntToken(text, start);
    }

    /** Consume the leading digit run; never sets isFloat. Always returns false (helper for chaining). */
    private boolean scanIntegerDigits() {
        while (i < src.length() && Character.isDigit(src.charAt(i))) {
            i++;
        }
        return false;
    }

    /** If a '.' followed by digits is present, consume it. Returns true if any fractional was found. */
    private boolean scanFractionalPart() {
        if (i >= src.length() || src.charAt(i) != '.') {
            return false;
        }
        i++;
        while (i < src.length() && Character.isDigit(src.charAt(i))) {
            i++;
        }
        return true;
    }

    /** If an exponent (e/E [+-] digits) is present, consume it. Returns true on consumption. */
    private boolean scanExponentPart() {
        if (i >= src.length()) {
            return false;
        }
        char c = src.charAt(i);
        if (c != 'e' && c != 'E') {
            return false;
        }
        i++;
        if (i < src.length()) {
            char sign = src.charAt(i);
            if (sign == '+' || sign == '-') {
                i++;
            }
        }
        while (i < src.length() && Character.isDigit(src.charAt(i))) {
            i++;
        }
        return true;
    }

    /** 2^53 — beyond this magnitude IEEE-754 doubles cannot represent successive integers
     *  exactly. Float literals with an integer part larger than this round silently, which
     *  is the precision-loss bypass the spec calls out. */
    private static final long IEEE_SAFE_INTEGER = 1L << 53;

    private Token buildFloatToken(String text, int start) {
        double d;
        try {
            d = Double.parseDouble(text);
        } catch (NumberFormatException e) {
            throw new PredicateValidationException("invalid numeric literal at " + start + ": " + text);
        }
        if (Double.isNaN(d) || Double.isInfinite(d)) {
            throw new PredicateValidationException("unsafe numeric literal (NaN/Infinity) at " + start);
        }
        rejectUnsafeFloatLiteral(text, d, start);
        return new Token(Kind.FLOAT_LITERAL, text, d, start);
    }

    /**
     * Reject float literals whose exact decimal value differs from the parsed {@code double} in
     * ways that would create a silent predicate bypass. The round-3 heuristic only inspected the
     * textual integer part before {@code .}/{@code e} — fine for {@code 9007199254740993.0}, but
     * blind to {@code 9.007199254740993e15} (integer part "9", 1 digit) which still parses to
     * the rounded value. We now compute the exact {@link BigDecimal} value of the literal and
     * apply two rules:
     *
     * <ol>
     *   <li><b>Integer-valued precision loss:</b> if the exact value is integral (no non-zero
     *       fractional component) and its absolute value exceeds {@code 2^53}, reject. A predicate
     *       {@code body.x == 9.007199254740993e15} otherwise matches a body containing the
     *       rounded {@code 9007199254740992}, not what the author wrote.
     *   <li><b>Subnormal underflow:</b> if the exact value is non-zero but the parsed double is
     *       {@code 0.0}, reject. A predicate {@code body.x == 1e-324} otherwise matches any record
     *       with {@code body.x == 0.0} — the author meant a tiny non-zero value, the runtime sees
     *       a confident match against zero.
     * </ol>
     *
     * <p>Decimal-shaped literals like {@code 0.1} or {@code 1.5e20} whose exact value is not
     * integral are NOT rejected — those carry inherent representation error users expect from
     * IEEE-754 and are not a silent-equality bypass vector.
     */
    private static void rejectUnsafeFloatLiteral(String text, double d, int start) {
        BigDecimal exact;
        try {
            exact = new BigDecimal(text);
        } catch (NumberFormatException e) {
            throw new PredicateValidationException("invalid numeric literal at " + start + ": " + text);
        }
        if (d == 0.0 && exact.signum() != 0) {
            throw new PredicateValidationException(
                    "unsafe numeric literal (underflow to zero) at " + start + ": " + text);
        }
        BigDecimal stripped = exact.stripTrailingZeros();
        if (stripped.scale() <= 0) {
            BigInteger magnitude = stripped.toBigIntegerExact().abs();
            if (magnitude.compareTo(BigInteger.valueOf(IEEE_SAFE_INTEGER)) > 0) {
                throw new PredicateValidationException(
                        "unsafe numeric literal (precision-loss territory) at " + start + ": " + text);
            }
        }
    }

    private Token buildIntToken(String text, int start) {
        long v;
        try {
            v = Long.parseLong(text);
        } catch (NumberFormatException e) {
            throw new PredicateValidationException(
                    "unsafe numeric literal at " + start + " (outside long range): " + text);
        }
        return new Token(Kind.INT_LITERAL, text, v, start);
    }

    private Token readString(int start, char quote) {
        i++; // consume opening quote
        StringBuilder sb = new StringBuilder();
        while (i < src.length()) {
            char c = src.charAt(i);
            if (c == quote) {
                i++;
                checkStringLength(sb);
                return new Token(Kind.STRING_LITERAL, src.substring(start, i), sb.toString(), start);
            }
            if (c == '\\' && i + 1 < src.length()) {
                appendEscapedChar(sb);
                checkStringLength(sb);
                continue;
            }
            // Reject raw NUL (U+0000) inside string literals. The body-path accessor cache in
            // RecordContexts uses NUL as a path-segment delimiter, so a string-literal accessor
            // containing NUL would collide with a dotted path: pathCacheKey(["a\u0000b"]) equals
            // pathCacheKey(["a","b"]) — both are "\0a\0b" — silently aliasing two distinct paths
            // to the same cache entry within one record evaluation. Predicates are an
            // access-control boundary; refusing the NUL byte at lex time prevents the alias
            // entirely instead of relying on the (admin-controlled) author to avoid it.
            if (c == '\u0000') {
                throw new PredicateValidationException(
                        "raw NUL (U+0000) in string literal at " + i + " (not permitted)");
            }
            sb.append(c);
            i++;
            checkStringLength(sb);
        }
        throw new PredicateValidationException("unterminated string literal starting at " + start);
    }

    private void appendEscapedChar(StringBuilder sb) {
        char next = src.charAt(i + 1);
        switch (next) {
            case '\\':
                sb.append('\\');
                break;
            case '\'':
                sb.append('\'');
                break;
            case '"':
                sb.append('"');
                break;
            case 'n':
                sb.append('\n');
                break;
            case 't':
                sb.append('\t');
                break;
            case 'r':
                sb.append('\r');
                break;
            default:
                throw new PredicateValidationException(
                        "invalid escape \\" + next + " in string literal at " + i);
        }
        i += 2;
    }

    private void checkStringLength(StringBuilder sb) {
        if (sb.length() > limits.maxStringLiteralLength) {
            throw new PredicateValidationException(
                    "string literal exceeds maxStringLiteralLength=" + limits.maxStringLiteralLength);
        }
    }

    private Token readSymbol(int start) {
        char c = src.charAt(i);
        char next = (i + 1 < src.length()) ? src.charAt(i + 1) : '\0';
        Token twoChar = tryTwoCharSymbol(c, next, start);
        if (twoChar != null) {
            return twoChar;
        }
        return readSingleCharSymbol(c, start);
    }

    private Token tryTwoCharSymbol(char c, char next, int start) {
        switch (c) {
            case '&':
                if (next == '&') {
                    i += 2;
                    return new Token(Kind.AND, "&&", null, start);
                }
                throw new PredicateValidationException("unexpected '&' at " + i + " (did you mean '&&'?)");
            case '|':
                if (next == '|') {
                    i += 2;
                    return new Token(Kind.OR, "||", null, start);
                }
                throw new PredicateValidationException("unexpected '|' at " + i + " (did you mean '||'?)");
            case '=':
                if (next == '=') {
                    if (i + 2 < src.length() && src.charAt(i + 2) == '=') {
                        throw new PredicateValidationException("invalid operator '===' at " + i);
                    }
                    i += 2;
                    return new Token(Kind.EQ, "==", null, start);
                }
                throw new PredicateValidationException("unexpected '=' at " + i + " (did you mean '=='?)");
            case '!':
                if (next == '=') {
                    i += 2;
                    return new Token(Kind.NEQ, "!=", null, start);
                }
                i++;
                return new Token(Kind.NOT, "!", null, start);
            case '<':
                if (next == '=') {
                    i += 2;
                    return new Token(Kind.LTE, "<=", null, start);
                }
                i++;
                return new Token(Kind.LT, "<", null, start);
            case '>':
                if (next == '=') {
                    i += 2;
                    return new Token(Kind.GTE, ">=", null, start);
                }
                i++;
                return new Token(Kind.GT, ">", null, start);
            default:
                return null;
        }
    }

    private Token readSingleCharSymbol(char c, int start) {
        switch (c) {
            case '+':
                i++;
                return new Token(Kind.PLUS, "+", null, start);
            case '-':
                i++;
                return new Token(Kind.MINUS, "-", null, start);
            case '*':
                i++;
                return new Token(Kind.STAR, "*", null, start);
            case '/':
                i++;
                return new Token(Kind.SLASH, "/", null, start);
            case '%':
                i++;
                return new Token(Kind.PERCENT, "%", null, start);
            case '.':
                i++;
                return new Token(Kind.DOT, ".", null, start);
            case '[':
                i++;
                return new Token(Kind.LBRACKET, "[", null, start);
            case ']':
                i++;
                return new Token(Kind.RBRACKET, "]", null, start);
            case '(':
                i++;
                return new Token(Kind.LPAREN, "(", null, start);
            case ')':
                i++;
                return new Token(Kind.RPAREN, ")", null, start);
            default:
                throw new PredicateValidationException("unexpected character '" + c + "' at " + i);
        }
    }
}
