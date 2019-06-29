/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.parser;

import static java.lang.String.format;

import io.confluent.ksql.parser.tree.NodeLocation;
import java.util.Optional;
import org.antlr.v4.runtime.RecognitionException;

public class ParsingException
    extends RuntimeException {

  private final int line;
  private final int charPositionInLine;

  public ParsingException(final String message) {
    this(message, null, 1, 0);
  }

  public ParsingException(final String message, final Optional<NodeLocation> nodeLocation) {
    this(
        message,
        null,
        nodeLocation.map(NodeLocation::getLineNumber).orElse(1),
        nodeLocation.map(NodeLocation::getColumnNumber).orElse(0)
    );
  }

  ParsingException(
      final String message,
      final RecognitionException cause,
      final int line,
      final int charPositionInLine
  ) {
    super(message, cause);

    this.line = line;
    this.charPositionInLine = charPositionInLine;
  }

  public int getLineNumber() {
    return line;
  }

  public int getColumnNumber() {
    return charPositionInLine + 1;
  }

  public String getErrorMessage() {
    return super.getMessage();
  }

  @Override
  public String getMessage() {
    return format("line %s:%s: %s", getLineNumber(), getColumnNumber(), getErrorMessage());
  }
}
