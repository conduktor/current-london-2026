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

import java.util.List;

/**
 * AST node types for the view predicate language. Nodes are immutable and shared across threads
 * once a predicate is compiled.
 *
 * Shape categories (used by compile-time type checks):
 *  - BOOL     — top-level guarantees boolean (literal bool, logical op, comparison, negation)
 *  - DYNAMIC  — type only known at runtime (literals number/string/null, identifier paths, arithmetic)
 */
final class Ast {
    private Ast() {}

    enum Shape { BOOL, DYNAMIC }

    abstract static class Node {
        abstract Shape shape();
    }

    /** Literal value: Long, Double, String, Boolean, or null. */
    static final class Literal extends Node {
        final Object value;
        Literal(Object value) {
            this.value = value;
        }
        @Override Shape shape() {
            return value instanceof Boolean ? Shape.BOOL : Shape.DYNAMIC;
        }
    }

    /** Path expression: root identifier followed by 0..N accessors. e.g. body.user.address[0]. */
    static final class Path extends Node {
        final String root;
        final List<String> accessors; // each accessor is a field name or quoted string key
        Path(String root, List<String> accessors) {
            this.root = root;
            this.accessors = List.copyOf(accessors);
        }
        @Override Shape shape() {
            return Shape.DYNAMIC;
        }
    }

    /** Binary operator: arithmetic, comparison, or logical. */
    static final class Binary extends Node {
        enum Op {
            // arithmetic — DYNAMIC shape
            ADD, SUB, MUL, DIV, MOD,
            // comparison — BOOL shape
            EQ, NEQ, LT, LTE, GT, GTE,
            // logical — BOOL shape
            AND, OR
        }
        final Op op;
        final Node left;
        final Node right;
        Binary(Op op, Node left, Node right) {
            this.op = op;
            this.left = left;
            this.right = right;
        }
        @Override Shape shape() {
            switch (op) {
                case AND: case OR:
                case EQ: case NEQ: case LT: case LTE: case GT: case GTE:
                    return Shape.BOOL;
                default:
                    return Shape.DYNAMIC;
            }
        }
    }

    /** Unary operator: logical NOT or numeric negation. */
    static final class Unary extends Node {
        enum Op { NOT, NEG }
        final Op op;
        final Node operand;
        Unary(Op op, Node operand) {
            this.op = op;
            this.operand = operand;
        }
        @Override Shape shape() {
            return op == Op.NOT ? Shape.BOOL : Shape.DYNAMIC;
        }
    }
}
