/**
 * Copyright 2017 Confluent Inc.
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
 **/

package io.confluent.ksql.parser;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import com.google.common.collect.ImmutableSet;

public class FormatterUtil {

  private static final Set<String> literalsSet = ImmutableSet.copyOf(
           IntStream.range(0, SqlBaseLexer.VOCABULARY.getMaxTokenType())
                  .mapToObj(SqlBaseLexer.VOCABULARY::getLiteralName)
                  .filter(Objects::nonNull)
                  // literals start and end with ' - remove them
                  .map(l -> l.substring(1, l.length() - 1))
                  .map(String::toUpperCase)
                  .collect(Collectors.toSet())
  );

  public static boolean isLiteral(String name) {
    return literalsSet.contains(name.toUpperCase());
  }

  public static String escapeIfLiteral(String name) {
    return isLiteral(name) ? "`" + name + "`" : name;
  }
}

