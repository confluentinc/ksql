/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.function;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.parser.SqlBaseParser;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Predicate;
import org.antlr.v4.runtime.Vocabulary;

/**
 * Check that a function name is valid. It is valid if it is not a Java reserved word
 * and not a ksql reserved word and is a valid java identifier.
 */
class FunctionNameValidator implements Predicate<String> {
  private static final Set<String> JAVA_RESERVED_WORDS
      = ImmutableSet.<String>builder()
      .add("abstract").add("assert").add("boolean").add("break").add("byte").add("case")
      .add("catch").add("char").add("class").add("const").add("continue").add("default")
      .add("double").add("else").add("enum").add("extends").add("do").add("final").add("finally")
      .add("float").add("for").add("goto").add("if").add("int").add("implements").add("import")
      .add("instanceof").add("interface").add("long").add("native").add("new").add("package")
      .add("private").add("public").add("protected").add("return").add("this").add("throw")
      .add("throws").add("transient").add("try").add("short").add("static").add("strictfp")
      .add("super").add("switch").add("synchronized").add("void").add("volatile").add("while")
      .build();


  // These are in the reserved words set, but we already use them for function names
  private static final Set<String> ALLOWED_KSQL_WORDS
      = ImmutableSet.copyOf(Arrays.asList("concat", "substring"));

  private static final Set<String> KSQL_RESERVED_WORDS = createFromVocabulary();


  private static Set<String> createFromVocabulary() {
    final Vocabulary vocabulary = SqlBaseParser.VOCABULARY;
    final int tokens = vocabulary.getMaxTokenType();
    final ImmutableSet.Builder<String> builder = ImmutableSet.builder();

    for (int i = 0; i < tokens; i++) {
      final String symbolicName = vocabulary.getSymbolicName(i);
      if (symbolicName != null) {
        final String keyWord = symbolicName.toLowerCase();
        if (!ALLOWED_KSQL_WORDS.contains(keyWord)) {
          builder.add(keyWord);
        }
      }
    }
    return builder.build();
  }

  @Override
  public boolean test(final String functionName) {
    if (functionName == null
        || functionName.trim().isEmpty()
        || JAVA_RESERVED_WORDS.contains(functionName.toLowerCase())
        || KSQL_RESERVED_WORDS.contains(functionName.toLowerCase())) {
      return false;
    }
    return isValidJavaIdentifier(functionName);

  }

  private boolean isValidJavaIdentifier(final String functionName) {
    final char [] characters = functionName.toCharArray();
    if (!Character.isJavaIdentifierStart((int)characters[0])) {
      return false;
    }

    for (int i = 1; i < characters.length; i++) {
      if (!Character.isJavaIdentifierPart((int)characters[i])) {
        return false;
      }
    }
    return true;
  }
}
