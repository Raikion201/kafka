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
package org.apache.kafka.connect.manifest.codegen.runtime.jinja;

/**
 * A lexical token produced by {@link Lexer}.
 *
 * <p>Tokens fall into three groups:
 * <ul>
 *   <li>Template-level: {@code RAW_TEXT}, {@code LSTACHE}/{@code RSTACHE} ({{...}}),
 *       {@code LSTMT}/{@code RSTMT} ({%...%}), {@code EOF}.</li>
 *   <li>Expression literals/identifiers: {@code IDENT}, {@code INTEGER},
 *       {@code FLOAT}, {@code STRING}, {@code TRUE}, {@code FALSE}, {@code NONE}.</li>
 *   <li>Expression operators &amp; punctuation, plus statement keywords
 *       ({@code IF}/{@code ELIF}/{@code ELSE}/{@code ENDIF}/{@code FOR}/{@code ENDFOR}/
 *       {@code SET}/{@code RAW}/{@code ENDRAW} and the test/membership keywords
 *       {@code AND}/{@code OR}/{@code NOT}/{@code IN}/{@code IS}).</li>
 * </ul>
 */
public final class Token {

    public enum Type {
        // template-level
        RAW_TEXT, LSTACHE, RSTACHE, LSTMT, RSTMT, EOF,

        // literals
        IDENT, INTEGER, FLOAT, STRING, TRUE, FALSE, NONE,

        // keywords (inside {{ ... }} or {% ... %})
        AND, OR, NOT, IN, IS,
        IF, ELIF, ELSE, ENDIF,
        FOR, ENDFOR,
        SET,
        RAW, ENDRAW,

        // punctuation / operators
        DOT, COMMA, COLON, PIPE, TILDE, ASSIGN,
        LPAREN, RPAREN, LBRACK, RBRACK, LBRACE, RBRACE,
        PLUS, MINUS, STAR, SLASH, PERCENT,
        EQ, NEQ, LT, GT, LE, GE
    }

    public final Type type;
    public final String text;
    public final int pos;

    public Token(Type type, String text, int pos) {
        this.type = type;
        this.text = text;
        this.pos = pos;
    }

    @Override
    public String toString() {
        return type + "(" + text + "@" + pos + ")";
    }
}
