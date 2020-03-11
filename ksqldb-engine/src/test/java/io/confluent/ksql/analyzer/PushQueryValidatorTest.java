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

package io.confluent.ksql.analyzer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.analyzer.Analysis.AliasedDataSource;
import io.confluent.ksql.analyzer.Analysis.Into;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.parser.tree.ResultMaterialization;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PushQueryValidatorTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private Analysis analysis;
  @Mock
  private AliasedDataSource aliasedSource;
  @Mock
  private DataSource source;
  @Mock
  private KsqlTopic topic;
  @Mock
  private KeyFormat keyFormat;

  private QueryValidator validator;

  @Before
  public void setUp() {
    validator = new PushQueryValidator();
  }

  @Test
  public void shouldThrowOnContinuousQueryThatIsFinal() {
    // Given:
    when(analysis.getResultMaterialization()).thenReturn(ResultMaterialization.FINAL);

    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Push queries don't support `EMIT FINAL`.");

    // When:
    validator.validate(analysis);
  }

  @Test
  public void shouldThrowOnPersistentPushQueryOnWindowedTable() {
    // Given:
    givenPushQuery();
    givenPersistentQuery();
    givenSourceTable();
    givenWindowedSource();

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "KSQL does not support persistent queries on windowed tables.");

    // When:
    validator.validate(analysis);
  }

  @Test
  public void shouldNotThrowOnTransientPushQueryOnWindowedTable() {
    // Given:
    givenPushQuery();
    givenTransientQuery();
    givenSourceTable();
    givenWindowedSource();

    // When/Then:
    validator.validate(analysis);
  }

  @Test
  public void shouldNotThrowOnPersistentPushQueryOnWindowedStream() {
    // Given:
    givenPushQuery();
    givenPersistentQuery();
    givenSourceStream();
    givenWindowedSource();

    // When/Then:
    validator.validate(analysis);
  }

  @Test
  public void shouldNotThrowOnPersistentPushQueryOnUnwindowedTable() {
    // Given:
    givenPushQuery();
    givenPersistentQuery();
    givenSourceTable();
    givenUnwindowedSource();

    // When/Then(don't throw):
    validator.validate(analysis);
  }

  private void givenPushQuery() {
    when(analysis.getResultMaterialization()).thenReturn(ResultMaterialization.CHANGES);
  }

  private void givenPersistentQuery() {
    when(analysis.getInto()).thenReturn(Optional.of(mock(Into.class)));
  }

  private void givenTransientQuery() {
    when(analysis.getInto()).thenReturn(Optional.empty());
  }

  @SuppressWarnings("unchecked")
  private void givenSourceTable() {
    when(analysis.getFromDataSources()).thenReturn(ImmutableList.of(aliasedSource));
    when(aliasedSource.getDataSource()).thenReturn(source);
    when(source.getDataSourceType()).thenReturn(DataSourceType.KTABLE);
  }

  @SuppressWarnings("unchecked")
  private void givenSourceStream() {
    when(analysis.getFromDataSources()).thenReturn(ImmutableList.of(aliasedSource));
    when(aliasedSource.getDataSource()).thenReturn(source);
    when(source.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
  }

  private void givenWindowedSource() {
    when(source.getKsqlTopic()).thenReturn(topic);
    when(topic.getKeyFormat()).thenReturn(keyFormat);
    when(keyFormat.isWindowed()).thenReturn(true);
  }

  private void givenUnwindowedSource() {
    when(source.getKsqlTopic()).thenReturn(topic);
    when(topic.getKeyFormat()).thenReturn(keyFormat);
    when(keyFormat.isWindowed()).thenReturn(false);
  }
}