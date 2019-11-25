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

package io.confluent.ksql.rest.server.validation;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.TerminateAllQueries;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TerminateAllQueriesValidatorTest {

  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(ImmutableMap.of());

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private PersistentQueryMetadata query0;
  @Mock
  private PersistentQueryMetadata query1;
  @Mock
  private KsqlEngine engine;
  @Mock
  private ServiceContext serviceContext;

  @Test
  public void shouldValidateTerminateAllQueries() {
    // Given:
    when(engine.getPersistentQueries()).thenReturn(ImmutableList.of(query0, query1));

    final ConfiguredStatement<TerminateAllQueries> statement = ConfiguredStatement.of(
        PreparedStatement.of("meh", new TerminateAllQueries(Optional.empty())),
        ImmutableMap.of(),
        KSQL_CONFIG
    );

    // When:
    CustomValidators.TERMINATE_ALL_QUERIES.validate(
        statement,
        ImmutableMap.of(),
        engine,
        serviceContext
    );

    // Then:
    verify(query0).close();
    verify(query1).close();
  }
}

