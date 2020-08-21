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

package io.confluent.ksql.metastore.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.test.util.ClassFinder;
import io.confluent.ksql.test.util.ImmutableTester;
import java.lang.reflect.Modifier;
import java.util.Collection;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Meta test to ensure all model classes meet certain requirements
 */
@SuppressWarnings("UnstableApiUsage")
@RunWith(Parameterized.class)
public class MetaStoreModelTest {

  private static final ImmutableMap<Class<?>, Object> DEFAULTS = ImmutableMap
      .<Class<?>, Object>builder()
      .put(KsqlTopic.class, new KsqlTopic(
          "bob",
          KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name())),
          ValueFormat.of(FormatInfo.of(FormatFactory.JSON.name()))
      ))
      .put(ColumnName.class, ColumnName.of("f0"))
      .put(SourceName.class, SourceName.of("f0"))
      .put(org.apache.kafka.connect.data.Field.class,
          new org.apache.kafka.connect.data.Field("bob", 1, Schema.OPTIONAL_STRING_SCHEMA))
      .put(Column.class, Column.of(ColumnName.of("someField"), SqlTypes.INTEGER, Namespace.VALUE, 1))
      .put(SqlType.class, SqlTypes.INTEGER)
      .put(LogicalSchema.class, LogicalSchema.builder()
          .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
          .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
          .build())
      .put(KeyFormat.class, KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name())))
      .put(ValueFormat.class, ValueFormat.of(FormatInfo.of(FormatFactory.JSON.name())))
      .put(SerdeOptions.class, SerdeOptions.of())
      .build();

  private final Class<?> modelClass;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Class<?>> data() {
    return ClassFinder.getClasses(StructuredDataSource.class.getPackage().getName());
  }

  public MetaStoreModelTest(final Class<?> modelClass) {
    this.modelClass = modelClass;
  }

  @Test
  public void shouldBeImmutable() {
    new ImmutableTester()
        .withKnownImmutableType(org.apache.kafka.connect.data.Field.class)
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