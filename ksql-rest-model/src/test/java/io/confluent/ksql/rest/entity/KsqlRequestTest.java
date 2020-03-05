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

package io.confluent.ksql.rest.entity;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlRequestConfig;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

@SuppressWarnings("SameParameterValue")
public class KsqlRequestTest {

  private static final ObjectMapper OBJECT_MAPPER = JsonMapper.INSTANCE.mapper;
  private static final String A_JSON_REQUEST = "{"
      + "\"ksql\":\"sql\","
      + "\"streamsProperties\":{"
      + "\"" + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "\":\"earliest\","
      + "\"" + StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG + "\":\""
                + TimestampExtractor.class.getCanonicalName() + "\""
      + "},"
      + "\"requestProperties\":{"
      + "\"" + KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING + "\":true"
      + "}}";
  private static final String A_JSON_REQUEST_WITH_COMMAND_NUMBER = "{"
      + "\"ksql\":\"sql\","
      + "\"streamsProperties\":{"
      + "\"" + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "\":\"earliest\","
      + "\"" + StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG + "\":\""
                + TimestampExtractor.class.getCanonicalName() + "\""
      + "},"
      + "\"requestProperties\":{"
      + "\"" + KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING + "\":true"
      + "},"
      + "\"commandSequenceNumber\":2,"
      + "\"isInternalRequest\":null}";
  private static final String A_JSON_REQUEST_WITH_NULL_COMMAND_NUMBER = "{"
      + "\"ksql\":\"sql\","
      + "\"streamsProperties\":{"
      + "\"" + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "\":\"earliest\","
      + "\"" + StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG + "\":\""
                + TimestampExtractor.class.getCanonicalName() + "\""
      + "},"
      + "\"requestProperties\":{"
      + "\"" + KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING + "\":true"
      + "},"
      + "\"commandSequenceNumber\":null,"
      + "\"isInternalRequest\":null}";

  private static final String A_JSON_REQUEST_WITH_NULL_INTERNAL_REQUEST= "{"
      + "\"ksql\":\"sql\","
      + "\"streamsProperties\":{"
      + "\"" + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "\":\"earliest\","
      + "\"" + StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG + "\":\""
      + TimestampExtractor.class.getCanonicalName() + "\""
      + "},"
      + "\"requestProperties\":{"
      + "\"" + KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING + "\":true"
      + "},"
      + "\"isInternalRequest\":null}";

  private static final String A_JSON_REQUEST_WITH_INTERNAL_REQUEST= "{"
      + "\"ksql\":\"sql\","
      + "\"streamsProperties\":{"
      + "\"" + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "\":\"earliest\","
      + "\"" + StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG + "\":\""
      + TimestampExtractor.class.getCanonicalName() + "\""
      + "},"
      + "\"isInternalRequest\":true}";

  private static final ImmutableMap<String, Object> SOME_PROPS = ImmutableMap.of(
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
      StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TimestampExtractor.class
  );
  private static final ImmutableMap<String, Object> SOME_REQUEST_PROPS = ImmutableMap.of(
      KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING, true
  );
  private static final long SOME_COMMAND_NUMBER = 2L;

  private static final KsqlRequest A_REQUEST = new KsqlRequest(
      "sql", SOME_PROPS, SOME_REQUEST_PROPS, null, null);
  private static final KsqlRequest A_REQUEST_WITH_COMMAND_NUMBER =
      new KsqlRequest("sql", SOME_PROPS, SOME_REQUEST_PROPS, SOME_COMMAND_NUMBER, null);
  private static final KsqlRequest A_REQUEST_WITH_INTERNAL_REQUEST =
      new KsqlRequest("sql", SOME_PROPS, Collections.emptyMap(), null, true);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldHandleNullStatement() {
    assertThat(new KsqlRequest(null, SOME_PROPS, SOME_REQUEST_PROPS, SOME_COMMAND_NUMBER, null).getKsql(),
        is(""));
  }

  @Test
  public void shouldHandleNullProps() {
    assertThat(new KsqlRequest("sql", null, SOME_REQUEST_PROPS, SOME_COMMAND_NUMBER, null).getConfigOverrides(),
        is(Collections.emptyMap()));
  }

  @Test
  public void shouldHandleNullCommandNumber() {
    assertThat(
      new KsqlRequest("sql", SOME_PROPS, Collections.emptyMap(), null, null).getCommandSequenceNumber(),
          is(Optional.empty()));
  }

  @Test
  public void shoudlHandleNullIsInternalRequest() {
    assertThat(new KsqlRequest("sql", SOME_PROPS, null, null, null).getIsInternalRequest(), is(Optional.empty()));
  }

  @Test
  public void shouldDeserializeFromJson() {
    // When:
    final KsqlRequest request = deserialize(A_JSON_REQUEST);

    // Then:
    assertThat(request, is(A_REQUEST));
  }

  @Test
  public void shouldDeserializeFromJsonWithNullInternalRequest() {
    // When:
    final KsqlRequest request = deserialize(A_JSON_REQUEST_WITH_NULL_INTERNAL_REQUEST);

    // Then:
    assertThat(request, is(A_REQUEST));
  }

  @Test
  public void shouldDeserializeFromJsonWithInternalRequest() {
    // When:
    final KsqlRequest request = deserialize(A_JSON_REQUEST_WITH_INTERNAL_REQUEST);

    // Then:
    assertThat(request, is(A_REQUEST_WITH_INTERNAL_REQUEST));
  }

  @Test
  public void shouldDeserializeFromJsonWithCommandNumber() {
    // When:
    final KsqlRequest request = deserialize(A_JSON_REQUEST_WITH_COMMAND_NUMBER);

    // Then:
    assertThat(request, is(A_REQUEST_WITH_COMMAND_NUMBER));
  }

  @Test
  public void shouldDeserializeFromJsonWithNullCommandNumber() {
    // When:
    final KsqlRequest request = deserialize(A_JSON_REQUEST_WITH_NULL_COMMAND_NUMBER);

    // Then:
    assertThat(request, is(A_REQUEST));
  }

  @Test
  public void shouldSerializeToJson() {
    // When:
    final String jsonRequest = serialize(A_REQUEST);

    // Then:
    assertThat(jsonRequest, is(A_JSON_REQUEST_WITH_NULL_COMMAND_NUMBER));
  }

  @Test
  public void shouldSerializeToJsonWithCommandNumber() {
    // When:
    final String jsonRequest = serialize(A_REQUEST_WITH_COMMAND_NUMBER);

    // Then:
    assertThat(jsonRequest, is(A_JSON_REQUEST_WITH_COMMAND_NUMBER));
  }

  @Test
  public void shouldImplementHashCodeAndEqualsCorrectly() {
    new EqualsTester()
        .addEqualityGroup(new KsqlRequest("sql", SOME_PROPS, SOME_REQUEST_PROPS, SOME_COMMAND_NUMBER, false),
            new KsqlRequest("sql", SOME_PROPS, SOME_REQUEST_PROPS, SOME_COMMAND_NUMBER, false))
        .addEqualityGroup(
            new KsqlRequest("different-sql", SOME_PROPS, SOME_REQUEST_PROPS, SOME_COMMAND_NUMBER, false))
        .addEqualityGroup(
            new KsqlRequest("sql", ImmutableMap.of(), SOME_REQUEST_PROPS, SOME_COMMAND_NUMBER, null))
        .addEqualityGroup(new KsqlRequest("sql", SOME_PROPS, SOME_REQUEST_PROPS, null, null))
        .testEquals();
  }

  @Test
  public void shouldHandleShortProperties() {
    // Given:
    final String jsonRequest = "{"
        + "\"ksql\":\"sql\","
        + "\"streamsProperties\":{"
        + "\"" + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "\":\"earliest\""
        + "}}";

    // When:
    final KsqlRequest request = deserialize(jsonRequest);

    // Then:
    assertThat(request.getConfigOverrides().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), equalTo("earliest"));
  }

  @Test
  public void shouldThrowOnInvalidPropertyValue() {
    // Given:
    final KsqlRequest request = new KsqlRequest(
        "sql",
        ImmutableMap.of(
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "not-parsable"
        ),
        SOME_REQUEST_PROPS,
        null,
        false);

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(containsString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    expectedException.expectMessage(containsString("not-parsable"));

    // When:
    request.getConfigOverrides();
  }

  @Test
  public void shouldHandleNullPropertyValue() {
    // Given:
    final KsqlRequest request = new KsqlRequest(
        "sql",
        Collections.singletonMap(
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        ),
        SOME_REQUEST_PROPS,
        null,
        false);

    // When:
    final Map<String, Object> props = request.getConfigOverrides();

    // Then:
    assertThat(props.keySet(), hasItem(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    assertThat(props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), is("earliest"));
  }

  @Test
  public void shouldHandleOverridesOfTypeList() {
    // Given:
    final KsqlRequest request = new KsqlRequest(
        "sql",
        ImmutableMap.of(
            ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ImmutableList.of("some.type")
        ),
        SOME_REQUEST_PROPS,
        null,
        false
    );

    // When:
    final Map<String, Object> props = request.getConfigOverrides();

    // Then:
    assertThat(
        props,
        hasEntry(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ImmutableList.of("some.type"))
    );
  }

  private static String serialize(final KsqlRequest request) {
    try {
      return OBJECT_MAPPER.writeValueAsString(request);
    } catch (final IOException e) {
      throw new RuntimeException("test invalid", e);
    }
  }

  private static KsqlRequest deserialize(final String json) {
    try {
      return OBJECT_MAPPER.readValue(json, KsqlRequest.class);
    } catch (final IOException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new RuntimeException(e);
    }
  }
}