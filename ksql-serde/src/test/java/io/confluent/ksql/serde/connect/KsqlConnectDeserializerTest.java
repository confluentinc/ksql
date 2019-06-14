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

package io.confluent.ksql.serde.connect;

import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.serde.SerdeTestUtils;
import io.confluent.ksql.serde.util.SerdeProcessingLogMessageFactory;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.Converter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class KsqlConnectDeserializerTest {
  private static final String TOPIC = "topic";
  private static final byte[] BYTES = "bizbazboz".getBytes(StandardCharsets.UTF_8);

  @Mock
  private Converter converter;
  @Mock
  private DataTranslator dataTranslator;
  @Mock
  private ProcessingLogger recordLogger;
  @Mock
  private Schema schema;
  @Mock
  private Object value;
  @Mock
  private Struct ksqlStruct;

  private final ProcessingLogConfig processingLogConfig
      = new ProcessingLogConfig(Collections.emptyMap());

  private KsqlConnectDeserializer connectDeserializer;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void setup() {
    connectDeserializer = new KsqlConnectDeserializer(
        converter,
        dataTranslator,
        recordLogger
    );
    when(converter.toConnectData(any(), any())).thenReturn(new SchemaAndValue(schema, value));
    when(dataTranslator.toKsqlRow(any(), any())).thenReturn(ksqlStruct);
  }

  @Test
  public void shouldDeserializeRecordsCorrectly() {
    // When:
    final Struct deserialized = (Struct) connectDeserializer.deserialize(TOPIC, BYTES);

    // Then:
    verify(converter, times(1)).toConnectData(TOPIC, BYTES);
    verify(dataTranslator, times(1)).toKsqlRow(schema, value);
    assertThat(deserialized, sameInstance(ksqlStruct));
  }

  @Test
  public void shouldLogOnError() {
    // Given:
    final RuntimeException error = new RuntimeException("bad");
    reset(converter);
    when(converter.toConnectData(any(), any())).thenThrow(error);

    // When:
   try {
     connectDeserializer.deserialize(TOPIC, BYTES);
     fail("deserialize should have thrown");
   } catch (final RuntimeException caught) {
     SerdeTestUtils.shouldLogError(
         recordLogger,
         SerdeProcessingLogMessageFactory.deserializationErrorMsg(
             error,
             Optional.ofNullable(BYTES)).apply(processingLogConfig),
         processingLogConfig
     );
   }
  }
}
