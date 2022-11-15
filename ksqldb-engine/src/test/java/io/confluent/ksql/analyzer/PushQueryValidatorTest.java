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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.analyzer.Analysis.AliasedDataSource;
import io.confluent.ksql.analyzer.Analysis.Into;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PushQueryValidatorTest {

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
  public void shouldThrowOnPersistentPushQueryOnWindowedTable() {
    // Given:
    givenPersistentQuery();
    givenSourceTable();
    givenWindowedSource();

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> validator.validate(analysis)
    );

    // Then:
    assertThat(e.getMessage(), containsString("KSQL does not support persistent queries on windowed tables."));
  }

  @Test
  public void shouldNotThrowOnTransientPushQueryOnWindowedTable() {
    // Given:
    givenTransientQuery();
    givenSourceTable();
    givenWindowedSource();

    // When/Then:
    validator.validate(analysis);
  }

  @Test
  public void shouldNotThrowOnPersistentPushQueryOnWindowedStream() {
    // Given:
    givenPersistentQuery();
    givenSourceStream();
    givenWindowedSource();

    // When/Then:
    validator.validate(analysis);
  }

  @Test
  public void shouldNotThrowOnPersistentPushQueryOnUnwindowedTable() {
    // Given:
    givenPersistentQuery();
    givenSourceTable();
    givenUnwindowedSource();

    // When/Then(don't throw):
    validator.validate(analysis);
  }

  private void givenPersistentQuery() {
    when(analysis.getInto()).thenReturn(Optional.of(mock(Into.class)));
  }

  private void givenTransientQuery() {
    when(analysis.getInto()).thenReturn(Optional.empty());
  }

  @SuppressWarnings("unchecked")
  private void givenSourceTable() {
    when(analysis.getAllDataSources()).thenReturn(ImmutableList.of(aliasedSource));
    when(aliasedSource.getDataSource()).thenReturn(source);
    when(source.getDataSourceType()).thenReturn(DataSourceType.KTABLE);
  }

  @SuppressWarnings("unchecked")
  private void givenSourceStream() {
    when(analysis.getAllDataSources()).thenReturn(ImmutableList.of(aliasedSource));
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