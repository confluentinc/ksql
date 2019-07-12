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

package io.confluent.ksql.util.timestamp;

import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public final class TimestampExtractionPolicyFactory {

  private TimestampExtractionPolicyFactory() {
  }

  public static TimestampExtractionPolicy create(
      final LogicalSchema schema,
      final Optional<String> timestampColumnName,
      final Optional<String> timestampFormat
  ) {
    if (!timestampColumnName.isPresent()) {
      return new MetadataTimestampExtractionPolicy();
    }

    final String fieldName = timestampColumnName.get().toUpperCase();

    final Field timestampField = schema.findValueField(fieldName)
        .orElseThrow(() -> new KsqlException(
            "The TIMESTAMP column set in the WITH clause does not exist in the schema: '"
                + fieldName + "'"));

    final Schema.Type timestampFieldType = timestampField.schema().type();
    if (timestampFieldType == Schema.Type.STRING) {

      final String format = timestampFormat.orElseThrow(() -> new KsqlException(
          "A String timestamp field has been specified without"
              + " also specifying the "
              + CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY.toLowerCase()));

      return new StringTimestampExtractionPolicy(fieldName, format);
    }

    if (timestampFormat.isPresent()) {
      throw new KsqlException("'" + CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY
          + "' set in the WITH clause can only be used "
          + "when the timestamp column in of type STRING.");
    }

    if (timestampFieldType == Schema.Type.INT64) {
      return new LongColumnTimestampExtractionPolicy(fieldName);
    }

    throw new KsqlException(
        "Timestamp column, " + timestampColumnName + ", should be LONG(INT64)"
            + " or a String with a "
            + CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY.toLowerCase()
            + " specified");
  }

}
