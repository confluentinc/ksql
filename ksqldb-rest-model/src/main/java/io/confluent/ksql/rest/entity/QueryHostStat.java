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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Objects;
import io.confluent.ksql.metrics.TopicSensors;
import java.io.IOException;

@JsonDeserialize(using = QueryHostStat.QueryHostStatDeserializer.class)
@JsonSerialize(using = QueryHostStat.QueryHostStatSerializer.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryHostStat extends TopicSensors.Stat {
  private final KsqlHostInfoEntity host;

  @JsonCreator
  public QueryHostStat(
      @JsonProperty("host") final KsqlHostInfoEntity host,
      @JsonProperty("name") final String name,
      @JsonProperty("value") final double value,
      @JsonProperty("timestamp") final long timestamp
  ) {
    super(name, value, timestamp);
    this.host = host;
  }

  public static QueryHostStat fromStat(
      final TopicSensors.Stat stat,
      final KsqlHostInfoEntity host
  ) {
    return new QueryHostStat(
        host,
        stat.name(),
        stat.getValue(),
        stat.getTimestamp()
    );
  }

  public KsqlHostInfoEntity host() {
    return host;
  }


  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryHostStat)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final QueryHostStat that = (QueryHostStat) o;
    return Objects.equal(host, that.host);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), host);
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class QueryHostStatDeserializer extends JsonDeserializer<QueryHostStat> {

    @Override
    public QueryHostStat deserialize(
        final JsonParser jp,
        final DeserializationContext ctxt
    ) throws IOException {
      final JsonNode node = jp.getCodec().readTree(jp);

      final String name = node.get("name").textValue();
      final double value = node.get("value").numberValue().doubleValue();
      final long timestamp = node.get("timestamp").numberValue().longValue();
      final KsqlHostInfoEntity host = new KsqlHostInfoEntity(node.get("host").textValue());
      return new QueryHostStat(host, name, value, timestamp);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class QueryHostStatSerializer extends JsonSerializer<QueryHostStat> {

    @Override
    public void serialize(
        final QueryHostStat stat,
        final JsonGenerator jsonGenerator,
        final SerializerProvider serializerProvider
    ) throws IOException {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeStringField("name", stat.name());
      jsonGenerator.writeStringField("host", stat.host().toString());
      jsonGenerator.writeObjectField("value", stat.getValue());
      jsonGenerator.writeNumberField("timestamp", stat.getTimestamp());
      jsonGenerator.writeEndObject();
    }
  }

}
