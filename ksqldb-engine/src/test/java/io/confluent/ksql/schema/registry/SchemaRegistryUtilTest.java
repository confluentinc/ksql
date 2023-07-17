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

package io.confluent.ksql.schema.registry;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SchemaRegistryUtilTest {

  private static final String APP_ID = "_my_app_id";

  @Mock
  private SchemaRegistryClient schemaRegistryClient;
  @Mock
  private SchemaMetadata schemaMetadata;

  @Test
  public void shouldDeleteChangeLogTopicSchema() throws Exception {
    // Given:
    when(schemaRegistryClient.getAllSubjects()).thenReturn(ImmutableList.of(
        APP_ID + "SOME-changelog-key",
        APP_ID + "SOME-changelog-value"
    ));

    // When:
    SchemaRegistryUtil.cleanupInternalTopicSchemas(APP_ID, schemaRegistryClient, false);

    // Then not exception:
    verify(schemaRegistryClient).deleteSubject(APP_ID + "SOME-changelog-key");
    verify(schemaRegistryClient).deleteSubject(APP_ID + "SOME-changelog-value");
  }

  @Test
  public void shouldDeleteRepartitionTopicSchema() throws Exception {
    // Given:
    when(schemaRegistryClient.getAllSubjects()).thenReturn(ImmutableList.of(
        APP_ID + "SOME-repartition-key",
        APP_ID + "SOME-repartition-value"
    ));

    // When:
    SchemaRegistryUtil.cleanupInternalTopicSchemas(APP_ID, schemaRegistryClient, false);

    // Then not exception:
    verify(schemaRegistryClient).deleteSubject(APP_ID + "SOME-repartition-key");
    verify(schemaRegistryClient).deleteSubject(APP_ID + "SOME-repartition-value");
  }

  @Test
  public void shouldHardDeleteIfFlagSet() throws Exception {
    // Given:
    when(schemaRegistryClient.getAllSubjects()).thenReturn(ImmutableList.of(
        APP_ID + "SOME-repartition-key",
        APP_ID + "SOME-repartition-value"
    ));

    // When:
    SchemaRegistryUtil.cleanupInternalTopicSchemas(APP_ID, schemaRegistryClient, true);

    // Then not exception:
    final InOrder inOrder = inOrder(schemaRegistryClient);
    inOrder.verify(schemaRegistryClient).deleteSubject(APP_ID + "SOME-repartition-key");
    inOrder.verify(schemaRegistryClient).deleteSubject(APP_ID + "SOME-repartition-key", true);
    inOrder.verify(schemaRegistryClient).deleteSubject(APP_ID + "SOME-repartition-value");
    inOrder.verify(schemaRegistryClient).deleteSubject(APP_ID + "SOME-repartition-value", true);
  }

  @Test
  public void shouldNotDeleteOtherSchemasForThisApplicationId() throws Exception {
    // Given:
    when(schemaRegistryClient.getAllSubjects()).thenReturn(ImmutableList.of(
        APP_ID + "SOME-other-key",
        APP_ID + "SOME-other-value"
    ));

    // When:
    SchemaRegistryUtil.cleanupInternalTopicSchemas(APP_ID, schemaRegistryClient, false);

    // Then not exception:
    verify(schemaRegistryClient, never()).deleteSubject(any());
  }

  @Test
  public void shouldNotDeleteOtherSchemas() throws Exception {
    // Given:
    when(schemaRegistryClient.getAllSubjects()).thenReturn(ImmutableList.of(
        "SOME-other-key",
        "SOME-other-value"
    ));

    // When:
    SchemaRegistryUtil.cleanupInternalTopicSchemas(APP_ID, schemaRegistryClient, false);

    // Then not exception:
    verify(schemaRegistryClient, never()).deleteSubject(any());
  }

  @Test
  public void shouldNotThrowIfAllSubjectsThrows() throws Exception {
    // Given:
    when(schemaRegistryClient.getAllSubjects()).thenThrow(new RuntimeException("Boom!"));

    // When:
    SchemaRegistryUtil.cleanupInternalTopicSchemas(APP_ID, schemaRegistryClient, false);

    // Then not exception:
    verify(schemaRegistryClient).getAllSubjects();
  }

  @Test
  public void shouldNotThrowIfDeleteSubjectThrows() throws Exception {
    // Given:
    when(schemaRegistryClient.getAllSubjects()).thenReturn(ImmutableList.of(
        APP_ID + "SOME-changelog-key",
        APP_ID + "SOME-changelog-value"
    ));

    when(schemaRegistryClient.deleteSubject(any())).thenThrow(new RuntimeException("Boom!"));

    // When:
    SchemaRegistryUtil.cleanupInternalTopicSchemas(APP_ID, schemaRegistryClient, false);

    // Then not exception:
    verify(schemaRegistryClient, times(5)).deleteSubject(APP_ID + "SOME-changelog-key");
    verify(schemaRegistryClient, times(5)).deleteSubject(APP_ID + "SOME-changelog-value");
  }

  @Test
  public void shouldNotRetryIf40401() throws Exception {
    // Given:
    when(schemaRegistryClient.getAllSubjects()).thenReturn(ImmutableList.of(
        APP_ID + "SOME-changelog-key",
        APP_ID + "SOME-changelog-value"
    ));

    when(schemaRegistryClient.deleteSubject(any())).thenThrow(new RestClientException("foo", 404, 40401));

    // When:
    SchemaRegistryUtil.cleanupInternalTopicSchemas(APP_ID, schemaRegistryClient, false);

    // Then not exception (only tried once):
    verify(schemaRegistryClient, times(1)).deleteSubject(APP_ID + "SOME-changelog-key");
    verify(schemaRegistryClient, times(1)).deleteSubject(APP_ID + "SOME-changelog-value");
  }

  @Test
  public void shouldReturnTrueOnIsSubjectExists() throws Exception {
    // Given:
    when(schemaRegistryClient.getLatestSchemaMetadata("foo-value")).thenReturn(schemaMetadata);

    // When:
    final boolean subjectExists = SchemaRegistryUtil.subjectExists(schemaRegistryClient, "foo-value");

    // Then:
    assertTrue("Expected subject to exist", subjectExists);
  }

  @Test
  public void shouldReturnFalseOnSubjectMissing() throws Exception {
    // Given:
    when(schemaRegistryClient.getLatestSchemaMetadata("bar-value")).thenThrow(
        new RestClientException("foo", 404, SchemaRegistryUtil.SUBJECT_NOT_FOUND_ERROR_CODE)
    );

    // When:
    final boolean subjectExists = SchemaRegistryUtil.subjectExists(schemaRegistryClient, "bar-value");

    // Then:
    assertFalse("Expected subject to not exist", subjectExists);
  }
}