/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.streams;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RegisterSchemaCallbackTest {

  private static final String SOURCE = "s1";
  private static final String CHANGELOG = "s2";
  private static final int ID = 1;
  private static final int ID2 = 2;
  private static final byte[] SOME_DATA = new byte[]{0x0, 0x0, 0x0, 0x0, 0x1};
  private static final byte[] OTHER_DATA = new byte[]{0x0, 0x0, 0x0, 0x0, 0x2};

  @Mock
  private SchemaRegistryClient srClient;
  @Mock
  private ParsedSchema schema;
  @Mock
  private ParsedSchema schema2;

  @Test
  public void shouldRegisterIdFromData() throws IOException, RestClientException {
    // Given:
    when(srClient.getSchemaBySubjectAndId(KsqlConstants.getSRSubject(SOURCE, false), ID)).thenReturn(schema);
    final RegisterSchemaCallback call = new RegisterSchemaCallback(srClient);

    // When:
    call.onDeserializationFailure(SOURCE, CHANGELOG, SOME_DATA);

    // Then:
    verify(srClient).register(KsqlConstants.getSRSubject(CHANGELOG, false), schema);
  }

  @Test
  public void shouldNotRegisterFailedIdTwice() throws IOException, RestClientException {
    // Given:
    when(srClient.getSchemaBySubjectAndId(KsqlConstants.getSRSubject(SOURCE, false), ID)).thenReturn(schema);
    when(srClient.register(KsqlConstants.getSRSubject(CHANGELOG, false), schema)).thenThrow(new KsqlException(""));
    final RegisterSchemaCallback call = new RegisterSchemaCallback(srClient);

    // When:
    call.onDeserializationFailure(SOURCE, CHANGELOG, SOME_DATA);
    call.onDeserializationFailure(SOURCE, CHANGELOG, SOME_DATA);

    // Then:
    verify(srClient, times(1)).getSchemaBySubjectAndId(KsqlConstants.getSRSubject(SOURCE, false), ID);
    verify(srClient).register(KsqlConstants.getSRSubject(CHANGELOG, false), schema);
  }

  @Test
  public void shouldRegisterOtherSchemaIdIfFirstFails() throws IOException, RestClientException {
    // Given:
    when(srClient.getSchemaBySubjectAndId(KsqlConstants.getSRSubject(SOURCE, false), ID2)).thenReturn(schema2);
    when(srClient.getSchemaBySubjectAndId(KsqlConstants.getSRSubject(SOURCE, false), ID)).thenReturn(schema);
    when(srClient.register(KsqlConstants.getSRSubject(CHANGELOG, false), schema)).thenThrow(new KsqlException(""));
    final RegisterSchemaCallback call = new RegisterSchemaCallback(srClient);

    // When:
    call.onDeserializationFailure(SOURCE, CHANGELOG, SOME_DATA);
    call.onDeserializationFailure(SOURCE, CHANGELOG, OTHER_DATA);

    // Then:
    verify(srClient, times(1)).getSchemaBySubjectAndId(KsqlConstants.getSRSubject(SOURCE, false), ID2);
    verify(srClient).register(KsqlConstants.getSRSubject(CHANGELOG, false), schema2);
  }

}