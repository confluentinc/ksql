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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.OngoingStubbing;

@RunWith(MockitoJUnitRunner.class)
public class TerminateQueryValidatorTest {

  @Rule public final TemporaryEngine engine = new TemporaryEngine();
  @Rule public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldFailOnTerminateUnknownQueryId() {
    // Expect:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expectMessage("Unknown queryId");

    // When:
    CustomValidators.TERMINATE_QUERY.validate(
        PreparedStatement.of("", new TerminateQuery("id")),
        engine.getEngine(),
        engine.getServiceContext(),
        engine.getKsqlConfig(),
        ImmutableMap.of()
    );
  }

  @Test
  public void shouldValidateKnownQueryId() {
    // Given:
    final PersistentQueryMetadata metadata = engine.givenPersistentQuery("id");
    final KsqlEngine mockEngine = mock(KsqlEngine.class);
    when(mockEngine.getPersistentQuery(any())).thenReturn(Optional.ofNullable(metadata));

    // Expect nothing when:
    CustomValidators.TERMINATE_QUERY.validate(
        PreparedStatement.of("", new TerminateQuery("id")),
        mockEngine,
        engine.getServiceContext(),
        engine.getKsqlConfig(),
        ImmutableMap.of()
    );
  }

}
