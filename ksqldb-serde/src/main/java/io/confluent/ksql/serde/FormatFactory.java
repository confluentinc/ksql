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

package io.confluent.ksql.serde;

import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.serde.delimited.DelimitedFormat;
import io.confluent.ksql.serde.json.JsonFormat;
import io.confluent.ksql.serde.json.JsonSchemaFormat;
import io.confluent.ksql.serde.kafka.KafkaFormat;
import io.confluent.ksql.serde.none.NoneFormat;
import io.confluent.ksql.serde.protobuf.ProtobufFormat;
import io.confluent.ksql.serde.protobuf.ProtobufNoSRFormat;
import io.confluent.ksql.util.KsqlException;

/**
 * A class containing the builtin supported formats in ksqlDB.
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public final class FormatFactory {

  public static final Format AVRO          = new AvroFormat();
  public static final Format JSON          = new JsonFormat();
  public static final Format JSON_SR       = new JsonSchemaFormat();
  public static final Format PROTOBUF      = new ProtobufFormat();
  public static final Format PROTOBUF_NOSR = new ProtobufNoSRFormat();
  public static final Format KAFKA         = new KafkaFormat();
  public static final Format DELIMITED     = new DelimitedFormat();
  public static final Format NONE          = new NoneFormat();

  private FormatFactory() {
  }

  /**
   * @param formatInfo the format specification
   * @return the corresponding {@code Format} if available
   * @throws KsqlException if the {@link FormatInfo#getFormat()} is not a builtin format in ksqlDB
   */
  public static Format of(final FormatInfo formatInfo) {
    final Format format = fromName(formatInfo.getFormat().toUpperCase());
    format.validateProperties(formatInfo.getProperties());
    return format;
  }

  public static Format fromName(final String name) {
    switch (name.toUpperCase()) {
      case AvroFormat.NAME:         return AVRO;
      case JsonFormat.NAME:         return JSON;
      case JsonSchemaFormat.NAME:   return JSON_SR;
      case ProtobufFormat.NAME:     return PROTOBUF;
      case ProtobufNoSRFormat.NAME: return PROTOBUF_NOSR;
      case KafkaFormat.NAME:        return KAFKA;
      case DelimitedFormat.NAME:    return DELIMITED;
      case NoneFormat.NAME:         return NONE;
      default:
        throw new KsqlException("Unknown format: " + name);
    }
  }
}
