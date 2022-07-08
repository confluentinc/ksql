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

package io.confluent.ksql.datagen;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import org.apache.avro.Schema;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

public class DataGenProducerTest {

  private static Schema getAvroSchema() throws IOException {
    return new Schema.Parser().parse(new File("./src/main/resources/pageviews_schema.avro"));
  }

  @Test
  public void shouldThrowIfTimestampColumnDoesNotExist() throws IOException {
    // When
    final IllegalArgumentException illegalArgumentException = assertThrows(
        IllegalArgumentException.class,
        () -> DataGenProducer.validateTimestampColumnType(Optional.of("page__id"), getAvroSchema())
    );

    // Then
    assertThat(illegalArgumentException.getMessage(),
        CoreMatchers.equalTo("The indicated timestamp field does not exist: page__id") );

  }

  @Test
  public void shouldThrowIfTimestampColumnTypeNotLong() throws IOException {
    // When
    final IllegalArgumentException illegalArgumentException = assertThrows(
        IllegalArgumentException.class,
        () -> DataGenProducer.validateTimestampColumnType(Optional.of("pageid"), getAvroSchema())
    );

    // Then
    assertThat(illegalArgumentException.getMessage(),
        CoreMatchers.equalTo("The timestamp column type should be bigint/long. pageid type is STRING") );

  }

}