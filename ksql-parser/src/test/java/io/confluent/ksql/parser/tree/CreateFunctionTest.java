/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.parser.tree;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.parser.tree.Type.SqlType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;

public class CreateFunctionTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);
  private static final QualifiedName SOME_NAME = QualifiedName.of("multiply");
  private static final QualifiedName SOME_OTHER_NAME = QualifiedName.of("divide");
  private static final String SOME_LANGUAGE = "JAVA";
  private static final String SOME_SCRIPT = "return X * Y;";
  private static final String SOME_OTHER_SCRIPT = "return X / 10;";
  private static final List<TableElement> SOME_ELEMENTS = ImmutableList.of(
    new TableElement("num1", PrimitiveType.of(SqlType.INTEGER)),
    new TableElement("num2", PrimitiveType.of(SqlType.INTEGER))
  );
  private static final List<TableElement> SOME_OTHER_ELEMENTS = ImmutableList.of(
    new TableElement("num1", PrimitiveType.of(SqlType.INTEGER))
  );
  private static final Map<String, Expression> SOME_PROPS = ImmutableMap.of(
      "author", new StringLiteral("mitch"),
      "description", new StringLiteral("multiply 2 numbers"),
      "version", new StringLiteral("0.1.0")
  );
  private static final Map<String, Expression> OTHER_PROPS = ImmutableMap.of(
      "author", new StringLiteral("mitchell"),
      "description", new StringLiteral("multiply 2 numbers!"),
      "version", new StringLiteral("0.2.0")
  );
  private static final Type SOME_RETURN_TYPE = PrimitiveType.of(SqlType.INTEGER);

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            // Note: At the moment location or optional function props do not take part in equality testing
            new CreateFunction(
                Optional.of(SOME_LOCATION),
                SOME_NAME,
                SOME_ELEMENTS,
                SOME_LANGUAGE,
                SOME_SCRIPT,
                SOME_RETURN_TYPE,
                SOME_PROPS,
                false),
            new CreateFunction(
                Optional.of(OTHER_LOCATION),
                SOME_NAME,
                SOME_ELEMENTS,
                SOME_LANGUAGE,
                SOME_SCRIPT,
                SOME_RETURN_TYPE,
                OTHER_PROPS,
                false)
        )
        .addEqualityGroup(
            new CreateFunction(
                Optional.of(OTHER_LOCATION),
                SOME_OTHER_NAME,
                SOME_ELEMENTS,
                SOME_LANGUAGE,
                SOME_SCRIPT,
                SOME_RETURN_TYPE,
                SOME_PROPS,
                false)
        )
        .addEqualityGroup(
            new CreateFunction(
                Optional.of(OTHER_LOCATION),
                SOME_OTHER_NAME,
                SOME_OTHER_ELEMENTS,
                SOME_LANGUAGE,
                SOME_OTHER_SCRIPT,
                SOME_RETURN_TYPE,
                SOME_PROPS,
                false)
        )
        .testEquals();
  }
}