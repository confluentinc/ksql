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

package io.confluent.ksql.topic;

import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.WindowInfo;
import java.time.Duration;
import java.util.Optional;

public final class TopicFactory {

  private TopicFactory() {
  }

  public static KsqlTopic create(final CreateSourceProperties properties) {
    final String kafkaTopicName = properties.getKafkaTopic();

    final Optional<WindowType> windowType = properties.getWindowType();
    final Optional<Duration> windowSize = properties.getWindowSize();

    final KeyFormat keyFormat = windowType
        .map(type -> KeyFormat
            .windowed(FormatInfo.of(Format.KAFKA), WindowInfo.of(type, windowSize)))
        .orElseGet(() -> KeyFormat
            .nonWindowed(FormatInfo.of(Format.KAFKA, Optional.empty())));

    final ValueFormat valueFormat = ValueFormat.of(FormatInfo.of(
        properties.getValueFormat(),
        properties.getValueAvroSchemaName()
    ));

    return new KsqlTopic(
        kafkaTopicName,
        keyFormat,
        valueFormat,
        false
    );
  }
}
