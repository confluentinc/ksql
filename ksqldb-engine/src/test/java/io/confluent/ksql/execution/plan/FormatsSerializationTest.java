/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.execution.plan;

import static io.confluent.ksql.serde.SerdeFeature.UNWRAP_SINGLES;
import static io.confluent.ksql.serde.SerdeFeature.WRAP_SINGLES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.json.PlanJsonMapper;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeatures;
import java.util.Map;
import org.junit.Test;

public class FormatsSerializationTest {

  private static final ObjectMapper MAPPER = PlanJsonMapper.INSTANCE.get();

  private static final Map<String, String> KEY_PROPS = ImmutableMap.of("key", "props");
  private static final Map<String, String> VAL_PROPS = ImmutableMap.of("val", "props");

  private static final FormatInfo KEY_FORMAT = FormatInfo.of("Vic", KEY_PROPS);
  private static final FormatInfo VAL_FORMAT = FormatInfo.of("Bob", VAL_PROPS);

  @Test
  public void shouldDeserializeOldStyleWrapSingleKeys() throws Exception {
    // Given:
    final String oldStyle = "{"
        + "\"keyFormat\":{\"format\":\"VIC\",\"properties\":{\"key\":\"props\"}},"
        + "\"valueFormat\":{\"format\":\"BOB\",\"properties\":{\"val\":\"props\"}},"
        + "\"options\":[\"WRAP_SINGLE_VALUES\"]"
        + "}";

    // When:
    final Formats result = MAPPER.readValue(oldStyle, Formats.class);

    // Then:
    assertThat(result, is(Formats.of(
        KEY_FORMAT, VAL_FORMAT, SerdeFeatures.of(), SerdeFeatures.of(WRAP_SINGLES)
    )));
  }

  @Test
  public void shouldDeserializeOldStyleUnwrapSingleKeys() throws Exception {
    // Given:
    final String oldStyle = "{"
        + "\"keyFormat\":{\"format\":\"VIC\",\"properties\":{\"key\":\"props\"}},"
        + "\"valueFormat\":{\"format\":\"BOB\",\"properties\":{\"val\":\"props\"}},"
        + "\"options\":[\"UNWRAP_SINGLE_VALUES\"]"
        + "}";

    // When:
    final Formats result = MAPPER.readValue(oldStyle, Formats.class);

    // Then:
    assertThat(result, is(Formats.of(
        KEY_FORMAT, VAL_FORMAT, SerdeFeatures.of(), SerdeFeatures.of(UNWRAP_SINGLES)
    )));
  }

  @Test
  public void shouldIncludeFeaturesIfNotEmpty() throws Exception {
    // Given:
    final Formats formats = Formats
        .of(KEY_FORMAT, VAL_FORMAT, SerdeFeatures.of(UNWRAP_SINGLES), SerdeFeatures.of(WRAP_SINGLES));

    // When:
    final String json = MAPPER.writeValueAsString(formats);

    // Then:
    assertThat(json, containsString("\"keyFeatures\":[\"UNWRAP_SINGLES\"]"));
    assertThat(json, containsString("\"valueFeatures\":[\"WRAP_SINGLES\"]"));
  }

  @Test
  public void shouldExcludeFeaturesIfEmpty() throws Exception {
    // Given:
    final Formats formats = Formats
        .of(KEY_FORMAT, VAL_FORMAT, SerdeFeatures.of(), SerdeFeatures.of());

    // When:
    final String json = MAPPER.writeValueAsString(formats);

    // Then:
    assertThat(json, not(containsString("Features")));
  }
}