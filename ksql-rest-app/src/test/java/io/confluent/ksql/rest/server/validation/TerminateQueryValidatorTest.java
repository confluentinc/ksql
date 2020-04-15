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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TerminateQueryValidatorTest {

  @Rule public final TemporaryEngine engine = new TemporaryEngine();

  @Test
  public void shouldFailOnTerminateUnknownQueryId() {
    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> CustomValidators.TERMINATE_QUERY.validate(
            ConfiguredStatement.of(
                PreparedStatement.of("", new TerminateQuery("id")),
                ImmutableMap.of(),
                engine.getKsqlConfig()
            ),
            ImmutableMap.of(),
            engine.getEngine(),
            engine.getServiceContext()
        )
    );

    // Then:
    assertThat(e.getRawMessage(), containsString("Unknown queryId"));
  }

  @Test
  public void shouldValidateKnownQueryId() {
    // Given:
    final PersistentQueryMetadata metadata = mock(PersistentQueryMetadata.class);
    final KsqlEngine mockEngine = mock(KsqlEngine.class);
    when(mockEngine.getPersistentQuery(any())).thenReturn(Optional.ofNullable(metadata));

    // Expect nothing when:
    CustomValidators.TERMINATE_QUERY.validate(
        ConfiguredStatement.of(
            PreparedStatement.of("", new TerminateQuery("id")),
            ImmutableMap.of(),
            engine.getKsqlConfig()
        ),
        ImmutableMap.of(),
        mockEngine,
        engine.getServiceContext()
    );
  }
}
