/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.test.parser;

import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.ParsingException;
import io.confluent.ksql.test.parser.TestDirective.Type;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.antlr.v4.runtime.Token;

public final class DirectiveParser {

  static final Pattern DIRECTIVE_REGEX = Pattern.compile(
      "--@(?<type>[a-zA-Z.]+):\\s*(?<contents>.*)"
  );

  private DirectiveParser() {
  }

  public static TestDirective parse(final Token comment) {
    final NodeLocation loc = new NodeLocation(comment.getLine(), comment.getCharPositionInLine());
    final Matcher matcher = DIRECTIVE_REGEX.matcher(comment.getText().trim());
    if (!matcher.find()) {
      throw new ParsingException(
          "Expected directive matching pattern " + DIRECTIVE_REGEX + " but got " + comment,
          loc.getStartLineNumber(),
          loc.getStartColumnNumber()
      );
    }

    final Type type = Type.from(matcher.group("type").toLowerCase());
    final String contents = matcher.group("contents");

    return new TestDirective(type, contents, loc);
  }
}
