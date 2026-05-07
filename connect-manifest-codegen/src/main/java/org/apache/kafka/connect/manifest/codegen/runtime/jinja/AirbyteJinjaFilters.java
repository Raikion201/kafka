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

import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.interpret.JinjavaInterpreter;
import com.hubspot.jinjava.lib.filter.Filter;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Airbyte CDK filter implementations that jinjava does not already ship, plus
 * overrides for filters whose Airbyte semantics diverge from jinjava's stock
 * behaviour. Mirrors {@code airbyte_cdk.sources.declarative.interpolation.filters}.
 *
 * <p>Filters added: {@code hash}, {@code hmac}, {@code regex_search},
 * {@code base64encode}, {@code base64decode}, {@code base64binascii_decode},
 * {@code string}. The rest of the Airbyte filter surface ({@code regex_replace},
 * etc.) is handled by jinjava's built-ins.</p>
 */
public final class AirbyteJinjaFilters {

    private AirbyteJinjaFilters() {
    }

    public static void registerAll(Jinjava jinjava) {
        jinjava.getGlobalContext().registerFilter(new HashFilter());
        jinjava.getGlobalContext().registerFilter(new HmacFilter());
        jinjava.getGlobalContext().registerFilter(new RegexSearchFilter());
        jinjava.getGlobalContext().registerFilter(new Base64EncodeFilter());
        jinjava.getGlobalContext().registerFilter(new Base64DecodeFilter());
        jinjava.getGlobalContext().registerFilter(new Base64BinasciiDecodeFilter());
        jinjava.getGlobalContext().registerFilter(new StringFilter());
        jinjava.getGlobalContext().registerFilter(new FloatFilter());
        jinjava.getGlobalContext().registerFilter(new IntFilter());
    }

    private static String asString(Object value) {
        return value == null ? "" : value.toString();
    }

    private static String pythonHashAlgo(String name) {
        // Airbyte exposes Python's hashlib names; map to JCE names.
        if (name == null) {
            return "MD5";
        }
        switch (name.toLowerCase(Locale.ROOT)) {
            case "md5":      return "MD5";
            case "sha1":     return "SHA-1";
            case "sha224":   return "SHA-224";
            case "sha256":   return "SHA-256";
            case "sha384":   return "SHA-384";
            case "sha512":   return "SHA-512";
            default:         return name;
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }

    /**
     * {@code value | hash(hash_type='md5', salt=None)}. Returns hex digest.
     * Mirrors {@code filters.py#hash}.
     */
    public static final class HashFilter implements Filter {
        @Override
        public String getName() {
            return "hash";
        }

        @Override
        public Object filter(Object var, JinjavaInterpreter interpreter, String... args) {
            String hashType = args.length > 0 ? args[0] : "md5";
            String salt = args.length > 1 ? args[1] : null;
            String input = asString(var);
            if (salt != null) {
                input = input + salt;
            }
            try {
                MessageDigest md = MessageDigest.getInstance(pythonHashAlgo(hashType));
                return hex(md.digest(input.getBytes(StandardCharsets.UTF_8)));
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalArgumentException("Unsupported hash type: " + hashType, e);
            }
        }
    }

    /**
     * {@code value | hmac(secret_key, hash_type='sha256')}. Returns hex digest.
     * Mirrors {@code filters.py#hmac}.
     */
    public static final class HmacFilter implements Filter {
        @Override
        public String getName() {
            return "hmac";
        }

        @Override
        public Object filter(Object var, JinjavaInterpreter interpreter, String... args) {
            if (args.length < 1) {
                throw new IllegalArgumentException("hmac filter requires a secret_key argument");
            }
            String secretKey = args[0];
            String hashType = args.length > 1 ? args[1] : "sha256";
            String jceName = "Hmac" + pythonHashAlgo(hashType).replace("-", "");
            try {
                Mac mac = Mac.getInstance(jceName);
                mac.init(new SecretKeySpec(secretKey.getBytes(StandardCharsets.UTF_8), jceName));
                byte[] out = mac.doFinal(asString(var).getBytes(StandardCharsets.UTF_8));
                return hex(out);
            } catch (Exception e) {
                throw new IllegalArgumentException("hmac failed: " + e.getMessage(), e);
            }
        }
    }

    /**
     * {@code value | regex_search(pattern)}. Returns the first capture group of
     * the first match, or empty string if no match. Mirrors
     * {@code filters.py#regex_search}.
     */
    public static final class RegexSearchFilter implements Filter {
        @Override
        public String getName() {
            return "regex_search";
        }

        @Override
        public Object filter(Object var, JinjavaInterpreter interpreter, String... args) {
            if (args.length < 1) {
                throw new IllegalArgumentException("regex_search requires a pattern argument");
            }
            Matcher m = Pattern.compile(args[0]).matcher(asString(var));
            if (!m.find()) {
                return "";
            }
            if (m.groupCount() >= 1) {
                String g = m.group(1);
                return g == null ? "" : g;
            }
            return m.group();
        }
    }

    /** {@code value | base64encode}. Standard Base64 of the UTF-8 bytes. */
    public static final class Base64EncodeFilter implements Filter {
        @Override
        public String getName() {
            return "base64encode";
        }

        @Override
        public Object filter(Object var, JinjavaInterpreter interpreter, String... args) {
            return Base64.getEncoder().encodeToString(asString(var).getBytes(StandardCharsets.UTF_8));
        }
    }

    /** {@code value | base64decode}. Decodes Base64 then interprets bytes as UTF-8. */
    public static final class Base64DecodeFilter implements Filter {
        @Override
        public String getName() {
            return "base64decode";
        }

        @Override
        public Object filter(Object var, JinjavaInterpreter interpreter, String... args) {
            byte[] bytes = Base64.getDecoder().decode(asString(var));
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    /**
     * {@code value | base64binascii_decode}. Decodes Base64 then interprets the
     * raw bytes as Latin-1 (binascii) so callers get every byte verbatim.
     * Airbyte uses this for binary-safe round-trips in HMAC headers.
     */
    public static final class Base64BinasciiDecodeFilter implements Filter {
        @Override
        public String getName() {
            return "base64binascii_decode";
        }

        @Override
        public Object filter(Object var, JinjavaInterpreter interpreter, String... args) {
            byte[] bytes = Base64.getDecoder().decode(asString(var));
            return new String(bytes, StandardCharsets.ISO_8859_1);
        }
    }

    /** {@code value | string}. Forces toString() — overrides jinjava's stock. */
    public static final class StringFilter implements Filter {
        @Override
        public String getName() {
            return "string";
        }

        @Override
        public Object filter(Object var, JinjavaInterpreter interpreter, String... args) {
            return asString(var);
        }
    }

    /**
     * {@code value | float}. Converts the value to a double.
     * Mirrors Python's {@code float()} built-in filter used in Airbyte manifests
     * for patterns like {@code config['lat']|float <= 90.0}.
     */
    public static final class FloatFilter implements Filter {
        @Override
        public String getName() {
            return "float";
        }

        @Override
        public Object filter(Object var, JinjavaInterpreter interpreter, String... args) {
            if (var == null) {
                return 0.0;
            }
            if (var instanceof Number n) {
                return n.doubleValue();
            }
            try {
                return Double.parseDouble(asString(var));
            } catch (NumberFormatException e) {
                return 0.0;
            }
        }
    }

    /**
     * {@code value | int}. Converts the value to a long integer.
     * Mirrors Python's {@code int()} built-in filter used in Airbyte manifests.
     */
    public static final class IntFilter implements Filter {
        @Override
        public String getName() {
            return "int";
        }

        @Override
        public Object filter(Object var, JinjavaInterpreter interpreter, String... args) {
            if (var == null) {
                return 0L;
            }
            if (var instanceof Number n) {
                return n.longValue();
            }
            try {
                return Long.parseLong(asString(var).trim());
            } catch (NumberFormatException e) {
                try {
                    return (long) Double.parseDouble(asString(var).trim());
                } catch (NumberFormatException ex) {
                    return 0L;
                }
            }
        }
    }
}
