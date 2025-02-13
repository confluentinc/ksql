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

package io.confluent.ksql.tools.test.parser;

import com.google.common.base.Functions;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.SqlBaseLexer;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A test directive indicates special handling of test-only metadata. The
 * directive is specified in the test sql file as a single-line comment that
 * begins with * {@code --@} and the directive type and contents are separated
 * by a colon. The type must contain only letters and "." characters.
 *
 * <p>For example, tests are delimited using the {@code TEST} directive, which
 * would be specified in a file using {@code --@test: name}. The directive types
 * are case-insensitive.</p>
 *
 * <p>The ANTLR grammar that parses comments will handle directives differently
 * from normal comments, and will send them to the {@link SqlBaseLexer#DIRECTIVES}
 * channel. See {@link SqlTestReader} for more handling of directives.</p>
 */
public class TestDirective {

  private final Type type;
  private final String contents;
  private final NodeLocation location;

  TestDirective(final Type type, final String contents, final NodeLocation location) {
    this.type = Objects.requireNonNull(type, "type");
    this.contents = Objects.requireNonNull(contents, "contents");
    this.location = Objects.requireNonNull(location, "location");
  }

  public Type getType() {
    return type;
  }

  public String getContents() {
    return contents;
  }

  public NodeLocation getLocation() {
    return location;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final TestDirective that = (TestDirective) o;
    return type == that.type
        && Objects.equals(contents, that.contents);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, contents);
  }

  @Override
  public String toString() {
    return "--@" + type.getTypeName() + ": " + contents;
  }

  public enum Type {
    TEST("test"),
    EXPECTED_ERROR("expected.error"),
    EXPECTED_MESSAGE("expected.message"),
    UNKNOWN("UNKNOWN")
    ;

    private static final Map<String, Type> NAME_MAP = Arrays
        .stream(Type.values())
        .collect(Collectors.toMap(Type::getTypeName, Functions.identity()));

    private final String typeName;

    Type(final String typeName) {
      this.typeName = Objects.requireNonNull(typeName, "typeName");
    }

    public String getTypeName() {
      return typeName;
    }

    public static Type from(final String typeName) {
      return NAME_MAP.getOrDefault(typeName, UNKNOWN);
    }
  }

}
