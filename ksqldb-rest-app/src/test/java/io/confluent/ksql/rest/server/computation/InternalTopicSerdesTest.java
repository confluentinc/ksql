/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.rest.server.computation;

import static io.confluent.ksql.rest.server.computation.InternalTopicSerdes.deserializer;
import static java.nio.charset.Charset.defaultCharset;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;

import com.google.common.base.Charsets;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.schema.Operator;
import java.nio.charset.Charset;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Test;

public class InternalTopicSerdesTest {

  private static final Expression EXPRESSION = new ArithmeticBinaryExpression(
      Operator.ADD,
      new IntegerLiteral(123),
      new IntegerLiteral(456)
  );

  @Test
  public void shouldUsePlanMapperForSerialize() {
    // When:
    final byte[] serialized = InternalTopicSerdes.serializer().serialize("", EXPRESSION);

    // Then:
    assertThat(new String(serialized, Charsets.UTF_8), equalTo("\"(123 + 456)\""));
  }

  @Test
  public void shouldUsePlanMapperForDeserialize() {
    // When:
    final Expression deserialized = InternalTopicSerdes.deserializer(Expression.class).deserialize(
        "",
        "\"(123 + 456)\"".getBytes(Charset.defaultCharset())
    );

    // Then:
    assertThat(deserialized, equalTo(EXPRESSION));
  }

  @Test
  public void shouldThrowSerializationExceptionOnSerializeError() {
    // When:
    assertThrows(
        SerializationException.class,
        () -> deserializer(Command.class).deserialize(
            "",
            "{abc".getBytes(defaultCharset())
        )
    );
  }
}
