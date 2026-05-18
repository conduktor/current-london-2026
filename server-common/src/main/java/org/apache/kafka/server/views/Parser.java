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

import org.apache.kafka.server.views.Lexer.Kind;
import org.apache.kafka.server.views.Lexer.Token;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Recursive-descent parser for the view predicate language. The grammar deliberately omits any
 * production that would let a user write a function call, comprehension, or regex match — so the
 * banned constructs cannot be expressed.
 *
 *   predicate := orExpr EOF
 *   orExpr    := andExpr ('||' andExpr)*
 *   andExpr   := cmpExpr ('&&' cmpExpr)*
 *   cmpExpr   := addExpr (('=='|'!='|'<'|'<='|'>'|'>=') addExpr)?
 *   addExpr   := mulExpr (('+'|'-') mulExpr)*
 *   mulExpr   := unaryExpr (('*'|'/'|'%') unaryExpr)*
 *   unaryExpr := ('!'|'-') unaryExpr | primary
 *   primary   := literal | identPath | '(' orExpr ')'
 *   identPath := IDENTIFIER ('.' IDENTIFIER | '[' STRING_LITERAL ']')*
 *
 * Two limits are enforced: maxNodes during parsing, and maxDepth measured post-parse as the
 * deepest chain of nested binary/unary nodes from root to leaf (so `(((body.x == 1)))` has
 * depth 1 — parens don't inflate the AST).
 */
final class Parser {
    /** Identifiers permitted at the root of a path. Anything else is rejected at parse time. */
    static final Set<String> ROOT_IDENTIFIERS = Set.of("body", "headers", "key", "offset", "partition", "timestamp");

    private final List<Token> tokens;
    private final PredicateLimits limits;
    private int pos = 0;
    private int nodeCount = 0;
    /**
     * Live count of open parentheses while parsing. Each '(' in primary increments this,
     * each ')' decrements. Capped by {@link PredicateLimits#maxParenDepth}. This is the
     * structural defense against `(((...)))` adversarial input: parens don't add AST nodes,
     * so neither {@code maxNodes} nor the post-parse {@code maxDepth} check would fire
     * before the recursive descent overflowed the JVM stack.
     */
    private int parenDepth = 0;

    Parser(List<Token> tokens, PredicateLimits limits) {
        this.tokens = tokens;
        this.limits = limits;
    }

    Ast.Node parse() {
        Ast.Node root = parseOr();
        expect(Kind.EOF);
        int depth = treeDepth(root);
        if (depth > limits.maxDepth) {
            throw new PredicateValidationException(
                    "predicate exceeds AST depth limit " + limits.maxDepth + " (got " + depth + ")");
        }
        return root;
    }

    private Ast.Node parseOr() {
        Ast.Node left = parseAnd();
        while (peek().kind == Kind.OR) {
            consume();
            Ast.Node right = parseAnd();
            left = node(new Ast.Binary(Ast.Binary.Op.OR, left, right));
        }
        return left;
    }

    private Ast.Node parseAnd() {
        Ast.Node left = parseCmp();
        while (peek().kind == Kind.AND) {
            consume();
            Ast.Node right = parseCmp();
            left = node(new Ast.Binary(Ast.Binary.Op.AND, left, right));
        }
        return left;
    }

    private Ast.Node parseCmp() {
        Ast.Node left = parseAdd();
        Token op = peek();
        Ast.Binary.Op cmp;
        switch (op.kind) {
            case EQ:
                cmp = Ast.Binary.Op.EQ;
                break;
            case NEQ:
                cmp = Ast.Binary.Op.NEQ;
                break;
            case LT:
                cmp = Ast.Binary.Op.LT;
                break;
            case LTE:
                cmp = Ast.Binary.Op.LTE;
                break;
            case GT:
                cmp = Ast.Binary.Op.GT;
                break;
            case GTE:
                cmp = Ast.Binary.Op.GTE;
                break;
            default:
                return left;
        }
        consume();
        Ast.Node right = parseAdd();
        return node(new Ast.Binary(cmp, left, right));
    }

    private Ast.Node parseAdd() {
        Ast.Node left = parseMul();
        while (peek().kind == Kind.PLUS || peek().kind == Kind.MINUS) {
            Ast.Binary.Op op = peek().kind == Kind.PLUS ? Ast.Binary.Op.ADD : Ast.Binary.Op.SUB;
            consume();
            Ast.Node right = parseMul();
            left = node(new Ast.Binary(op, left, right));
        }
        return left;
    }

    private Ast.Node parseMul() {
        Ast.Node left = parseUnary();
        while (peek().kind == Kind.STAR || peek().kind == Kind.SLASH || peek().kind == Kind.PERCENT) {
            Ast.Binary.Op op;
            switch (peek().kind) {
                case STAR:
                    op = Ast.Binary.Op.MUL;
                    break;
                case SLASH:
                    op = Ast.Binary.Op.DIV;
                    break;
                case PERCENT:
                    op = Ast.Binary.Op.MOD;
                    break;
                default:
                    throw new IllegalStateException();
            }
            consume();
            Ast.Node right = parseUnary();
            left = node(new Ast.Binary(op, left, right));
        }
        return left;
    }

    private Ast.Node parseUnary() {
        if (peek().kind == Kind.NOT) {
            consume();
            return node(new Ast.Unary(Ast.Unary.Op.NOT, parseUnary()));
        }
        if (peek().kind == Kind.MINUS) {
            consume();
            return node(new Ast.Unary(Ast.Unary.Op.NEG, parseUnary()));
        }
        return parsePrimary();
    }

    private Ast.Node parsePrimary() {
        Token t = peek();
        switch (t.kind) {
            case INT_LITERAL:
            case FLOAT_LITERAL:
            case STRING_LITERAL:
            case TRUE:
            case FALSE:
            case NULL:
                consume();
                return node(new Ast.Literal(t.value));
            case LPAREN: {
                consume();
                parenDepth++;
                if (parenDepth > limits.maxParenDepth) {
                    throw new PredicateValidationException(
                            "predicate exceeds maxParenDepth=" + limits.maxParenDepth
                                    + " at position " + t.pos);
                }
                Ast.Node inner = parseOr();
                expect(Kind.RPAREN);
                parenDepth--;
                return inner;
            }
            case LBRACKET:
                throw new PredicateValidationException(
                        "list/comprehension syntax is not supported at position " + t.pos);
            case IDENTIFIER:
                return parseIdentPath();
            default:
                throw new PredicateValidationException(
                        "unexpected token " + t.kind + " '" + t.text + "' at position " + t.pos);
        }
    }

    private Ast.Node parseIdentPath() {
        Token root = consume(); // IDENTIFIER
        // Catch function-call syntax explicitly with a useful message rather than a generic syntax error.
        if (peek().kind == Kind.LPAREN) {
            throw new PredicateValidationException(
                    "function call '" + root.text + "(' is not allowed (comprehensions, regex, and "
                            + "user-defined functions are banned for safety) at position " + root.pos);
        }
        if (!ROOT_IDENTIFIERS.contains(root.text)) {
            throw new PredicateValidationException(
                    "unknown identifier '" + root.text + "' at position " + root.pos
                            + " (allowed: " + ROOT_IDENTIFIERS + ")");
        }
        List<String> accessors = new ArrayList<>();
        while (true) {
            if (peek().kind == Kind.DOT) {
                consume();
                Token name = expect(Kind.IDENTIFIER);
                if (peek().kind == Kind.LPAREN) {
                    throw new PredicateValidationException(
                            "function call '" + name.text + "(' is not allowed (method calls and "
                                    + "comprehensions like .exists/.filter/.map/.matches are banned) at position "
                                    + name.pos);
                }
                accessors.add(name.text);
            } else if (peek().kind == Kind.LBRACKET) {
                consume();
                Token key = expect(Kind.STRING_LITERAL);
                expect(Kind.RBRACKET);
                accessors.add((String) key.value);
            } else {
                break;
            }
        }
        return node(new Ast.Path(root.text, accessors));
    }

    // ---------- helpers ----------

    private Token peek() {
        return tokens.get(pos);
    }

    private Token consume() {
        return tokens.get(pos++);
    }

    private Token expect(Kind kind) {
        Token t = peek();
        if (t.kind != kind) {
            throw new PredicateValidationException(
                    "expected " + kind + " but found " + t.kind + " '" + t.text + "' at position " + t.pos);
        }
        return consume();
    }

    private Ast.Node node(Ast.Node n) {
        nodeCount++;
        if (nodeCount > limits.maxNodes) {
            throw new PredicateValidationException(
                    "predicate exceeds AST node limit " + limits.maxNodes);
        }
        return n;
    }

    private static int treeDepth(Ast.Node n) {
        if (n instanceof Ast.Binary) {
            Ast.Binary b = (Ast.Binary) n;
            return 1 + Math.max(treeDepth(b.left), treeDepth(b.right));
        }
        if (n instanceof Ast.Unary) {
            return 1 + treeDepth(((Ast.Unary) n).operand);
        }
        return 1;
    }

    /** Visible for tests / debug only. */
    int parsedNodeCount() {
        return nodeCount;
    }
}
