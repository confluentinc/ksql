/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.parser.tree;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.HashMap;
import java.util.Optional;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CreateSourcePropertiesTest {

  private static final java.util.Map<String, Literal> MINIMUM_VALID_PROPS = ImmutableMap.of(
      DdlConfig.VALUE_FORMAT_PROPERTY, new StringLiteral("AvRo"),
      DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("foo")
  );

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldSetMinimumValidProps() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(MINIMUM_VALID_PROPS);

    // Then:
    assertThat(properties.getKafkaTopic(), is("foo"));
    assertThat(properties.getValueFormat(), is(Format.AVRO));
  }

  @Test
  public void shouldReturnOptionalEmptyForMissingProps() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(MINIMUM_VALID_PROPS);

    // Then:
    assertThat(properties.getKeyField(), is(Optional.empty()));
    assertThat(properties.getTimestampName(), is(Optional.empty()));
    assertThat(properties.getTimestampFormat(), is(Optional.empty()));
    assertThat(properties.getWindowType(), is(Optional.empty()));
    assertThat(properties.getAvroSchemaId(), is(Optional.empty()));
    assertThat(properties.getValueAvroSchemaName(), is(Optional.empty()));
    assertThat(properties.getReplicas(), is(Optional.empty()));
    assertThat(properties.getPartitions(), is(Optional.empty()));
    assertThat(properties.getWrapSingleValues(), is(Optional.empty()));
  }

  @Test
  public void shouldSetValidKey() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(DdlConfig.KEY_NAME_PROPERTY, new StringLiteral("key"))
            .build());

    // Then:
    assertThat(properties.getKeyField(), is(Optional.of("key")));
  }

  @Test
  public void shouldSetValidTimestampName() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(DdlConfig.TIMESTAMP_NAME_PROPERTY, new StringLiteral("ts"))
            .build());

    // Then:
    assertThat(properties.getTimestampName(), is(Optional.of("ts")));
  }

  @Test
  public void shouldSetValidTimestampFormat() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(DdlConfig.TIMESTAMP_FORMAT_PROPERTY, new StringLiteral("ts"))
            .build());

    // Then:
    assertThat(properties.getTimestampFormat(), is(Optional.of("ts")));
  }

  @Test
  public void shouldSetValidWindowType() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(DdlConfig.WINDOW_TYPE_PROPERTY, new StringLiteral("HoPPinG"))
            .build());

    // Then:
    assertThat("hasWindowType", properties.getWindowType().isPresent());
    assertThat(
        properties.getWindowType().get().create(),
        instanceOf(WindowedSerdes.timeWindowedSerdeFrom(String.class).getClass()));
  }

  @Test
  public void shouldSetValidSchemaId() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(KsqlConstants.AVRO_SCHEMA_ID, new StringLiteral("1"))
            .build());

    // Then:
    assertThat(properties.getAvroSchemaId(), is(Optional.of(1)));
  }

  @Test
  public void shouldSetValidAvroSchemaName() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(DdlConfig.VALUE_AVRO_SCHEMA_FULL_NAME, new StringLiteral("schema"))
            .build());

    // Then:
    assertThat(properties.getValueAvroSchemaName(), is(Optional.of("schema")));
  }

  @Test
  public void shouldCleanQuotesForStrings() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(DdlConfig.VALUE_AVRO_SCHEMA_FULL_NAME, new StringLiteral("'schema'"))
            .build());

    // Then:
    assertThat(properties.getValueAvroSchemaName(), is(Optional.of("schema")));
  }

  @Test
  public void shouldSetReplicasFromNumber() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(KsqlConstants.SOURCE_NUMBER_OF_REPLICAS, new IntegerLiteral(2))
            .build());

    // Then:
    assertThat(properties.getReplicas(), is(Optional.of((short) 2)));
  }

  @Test
  public void shouldSetPartitions() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(KsqlConstants.SOURCE_NUMBER_OF_PARTITIONS, new IntegerLiteral(2))
            .build());

    // Then:
    assertThat(properties.getPartitions(), is(Optional.of(2)));
  }

  @Test
  public void shouldSetWrapSingleValues() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(DdlConfig.WRAP_SINGLE_VALUE, new BooleanLiteral("true"))
            .build());

    // Then:
    assertThat(properties.getWrapSingleValues(), is(Optional.of(true)));
  }

  @Test
  public void shouldSetNumericPropertyFromStringLiteral() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(KsqlConstants.SOURCE_NUMBER_OF_REPLICAS, new StringLiteral("3"))
            .build());

    // Then:
    assertThat(properties.getReplicas(), is(Optional.of((short) 3)));
  }

  @Test
  public void shouldSetBooleanPropertyFromStringLiteral() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(DdlConfig.WRAP_SINGLE_VALUE, new StringLiteral("true"))
            .build());

    // Then:
    assertThat(properties.getWrapSingleValues(), is(Optional.of(true)));
  }

  @Test
  public void shouldHandleNonUpperCasePropNames() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(DdlConfig.WRAP_SINGLE_VALUE.toLowerCase(), new StringLiteral("false"))
            .build());

    // Then:
    assertThat(properties.getWrapSingleValues(), is(Optional.of(false)));
  }

  @Test
  public void shouldFailIfNoKafkaTopicName() {
    // Given:
    final HashMap<String, Literal> props = new HashMap<>(MINIMUM_VALID_PROPS);
    props.remove(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY);

    // Expect:
    expectedException.expectMessage(
        "Missing required property \"KAFKA_TOPIC\" which has no default value.");
    expectedException.expect(KsqlException.class);

    // When:
    CreateSourceProperties.from(props);
  }

  @Test
  public void shouldFailIfNoValueFormat() {
    // Given:
    final HashMap<String, Literal> props = new HashMap<>(MINIMUM_VALID_PROPS);
    props.remove(DdlConfig.VALUE_FORMAT_PROPERTY);

    // Expect:
    expectedException
        .expectMessage("Missing required property \"VALUE_FORMAT\" which has no default value.");
    expectedException.expect(KsqlException.class);

    // When:
    CreateSourceProperties.from(props);
  }

  @Test
  public void shouldFailIfInvalidWindowConfig() {
    // Expect:
    expectedException.expectMessage(
        "Invalid value bar for property WINDOW_TYPE: String must be one of: SESSION, HOPPING, TUMBLING");
    expectedException.expect(KsqlException.class);

    // When:
    CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(DdlConfig.WINDOW_TYPE_PROPERTY, new StringLiteral("bar"))
            .build()
    );
  }

  @Test
  public void shouldFailIfInvalidConfig() {
    // Expect:
    expectedException.expectMessage("Invalid config variable(s) in the WITH clause: FOO");
    expectedException.expect(KsqlException.class);

    // When:
    CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put("foo", new StringLiteral("bar"))
            .build()
    );
  }

  @Test
  public void shouldProperlyImplementEqualsAndHashCode() {
    new EqualsTester()
        .addEqualityGroup(
            CreateSourceProperties.from(MINIMUM_VALID_PROPS),
            CreateSourceProperties.from(MINIMUM_VALID_PROPS))
        .addEqualityGroup(
            CreateSourceProperties.from(ImmutableMap.<String, Literal>builder()
                .putAll(MINIMUM_VALID_PROPS)
                .put(DdlConfig.VALUE_AVRO_SCHEMA_FULL_NAME, new StringLiteral("schema"))
                .build()))
        .testEquals();
  }

  @Test
  public void shouldIncludeOnlyProvidedPropsInToString() {
    // Given:
    final CreateSourceProperties props = CreateSourceProperties
        .from(ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put("Wrap_Single_value", new StringLiteral("True"))
            .build());

    // When:
    final String sql = props.toString();

    // Then:
    assertThat(sql, is("KAFKA_TOPIC='foo', VALUE_FORMAT='AvRo', WRAP_SINGLE_VALUE='True'"));
  }

  @Test
  public void shouldNotQuoteNonStringPropValues() {
    // Given:
    final CreateSourceProperties props = CreateSourceProperties
        .from(ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put("Wrap_Single_value", new BooleanLiteral("true"))
            .build());

    // When:
    final String sql = props.toString();

    // Then:
    assertThat(sql, containsString("WRAP_SINGLE_VALUE=true"));
  }
}