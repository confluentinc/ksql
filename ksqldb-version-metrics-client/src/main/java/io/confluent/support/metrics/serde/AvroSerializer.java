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

package io.confluent.support.metrics.serde;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;

public class AvroSerializer {

  /**
   * Serializes the record as an in-memory representation of a standard Avro file.
   *
   * <p>That is, the returned bytes include a standard Avro header that contains a magic byte, the
   * record's Avro schema (and so on), followed by the byte representation of the record.
   *
   * <p>Implementation detail:  This method uses Avro's {@code DataFileWriter}.
   *
   * @return Avro-encoded record (bytes) that includes the Avro schema
   */
  public byte[] serialize(final GenericContainer record) throws IOException {
    if (record != null) {
      final DatumWriter<GenericContainer> datumWriter = new GenericDatumWriter<>(
          record.getSchema());
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      final DataFileWriter<GenericContainer> writer = new DataFileWriter<>(datumWriter);
      writer.create(record.getSchema(), out);
      writer.append(record);
      writer.close();
      out.close();
      return out.toByteArray();
    } else {
      return null;
    }
  }

}
