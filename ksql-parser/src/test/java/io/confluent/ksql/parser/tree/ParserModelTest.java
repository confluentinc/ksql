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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.test.util.ClassFinder;
import io.confluent.ksql.test.util.ImmutableTester;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Window;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Meta test to ensure all model classes meet certain requirements
 */
@RunWith(Parameterized.class)
public class ParserModelTest {

  private static final Select DEFAULT_SELECT =
      new Select(ImmutableList.of(new AllColumns(Optional.empty())));
  private static final Table DEFAULT_RELATION = new Table(QualifiedName.of("vic"));
  private static final Type DEFAULT_TYPE = new Type(SqlTypes.STRING);

  private static final ImmutableMap<Class<?>, Object> DEFAULTS = ImmutableMap
      .<Class<?>, Object>builder()
      .put(QualifiedName.class, QualifiedName.of("bob"))
      .put(Expression.class, DEFAULT_TYPE)
      .put(KsqlWindowExpression.class, new TumblingWindowExpression(1, TimeUnit.SECONDS))
      .put(Relation.class, DEFAULT_RELATION)
      .put(JoinCriteria.class, new JoinOn(DEFAULT_TYPE))
      .put(Select.class, DEFAULT_SELECT)
      .put(InListExpression.class, new InListExpression(ImmutableList.of(DEFAULT_TYPE)))
      .put(io.confluent.ksql.parser.tree.Type.class, DEFAULT_TYPE)
      .put(Query.class, new Query(
          DEFAULT_SELECT,
          DEFAULT_RELATION,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          OptionalInt.empty()))
      .put(java.util.Map.class,
          ImmutableMap.of(
              DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("topic_test"),
              DdlConfig.VALUE_FORMAT_PROPERTY, new StringLiteral("avro")
          ))
      .put(TableElements.class, TableElements.of())
      .put(SqlType.class, SqlTypes.BIGINT)
      .build();

  private final Class<?> modelClass;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Class<?>> data() {
    return ClassFinder.getClasses(FunctionCall.class.getPackage().getName()).stream()
        .filter(Node.class::isAssignableFrom)
        .collect(Collectors.toList());
  }

  public ParserModelTest(final Class<?> modelClass) {
    this.modelClass = modelClass;
  }

  @Test
  public void shouldBeImmutable() {
    new ImmutableTester()
        .withKnownImmutableType(Window.class)
        .withKnownImmutableType(JoinWindows.class)
        .withKnownImmutableType(ConfigDef.class)
        .withKnownImmutableType(AbstractConfig.class)
        .test(modelClass);
  }

  @Test
  public void shouldThrowNpeFromConstructors() {
    assumeThat(Modifier.isAbstract(modelClass.getModifiers()), is(false));

    getNullPointerTester()
        .testConstructors(modelClass, Visibility.PACKAGE);
  }

  @Test
  public void shouldThrowNpeFromFactoryMethods() {
    getNullPointerTester()
        .testStaticMethods(modelClass, Visibility.PACKAGE);
  }

  @SuppressWarnings({"unchecked", "UnstableApiUsage"})
  private static NullPointerTester getNullPointerTester() {
    final NullPointerTester tester = new NullPointerTester();
    DEFAULTS.forEach((type, value) -> {
      assertThat(value, is(instanceOf(type)));
      tester.setDefault((Class) type, value);
    });
    return tester;
  }
}
