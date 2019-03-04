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

package io.confluent.ksql.serde.delimited;

import io.confluent.ksql.GenericRow;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;


public class KsqlDelimitedSerializer implements Serializer<GenericRow> {

  private final Schema schema;
  private final CSVFormat csvFormat;

  public KsqlDelimitedSerializer(final Schema schema, final CSVFormat csvFormat) {
    this.schema = schema;
    this.csvFormat = csvFormat;
  }

  @Override
  public void configure(final Map<String, ?> map, final boolean b) {

  }

  @Override
  public byte[] serialize(final String topic, final GenericRow genericRow) {
    if (genericRow == null) {
      return null;
    }
    try {
      final StringWriter stringWriter = new StringWriter();
      final CSVPrinter csvPrinter = new CSVPrinter(stringWriter, csvFormat);
      csvPrinter.printRecord(genericRow.getColumns());
      final String result = stringWriter.toString();
      return result.substring(0, result.length() - 2).getBytes(StandardCharsets.UTF_8);
    } catch (final Exception e) {
      throw new SerializationException("Error serializing CSV message", e);
    }

  }

  @Override
  public void close() {

  }
}
