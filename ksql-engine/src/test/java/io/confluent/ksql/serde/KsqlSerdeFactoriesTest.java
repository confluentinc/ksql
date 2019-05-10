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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.parser.tree.CreateSourceProperties;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.serde.avro.KsqlAvroSerdeFactory;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlSerdeFactoriesTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private CreateSourceProperties statementProps;
  @Mock
  private Map<String, Expression> sinkProps;

  private SerdeFactories factory;

  @Before
  public void setUp() {
    factory = new KsqlSerdeFactories();
  }

  @Test
  public void shouldThrowOnJsonIfValuesAvroSchemaNameSet() {
    // Given:
    when(statementProps.getValueAvroSchemaName()).thenReturn(Optional.of("vic"));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("VALUE_AVRO_SCHEMA_FULL_NAME is only valid for AVRO topics.");

    // When:
    factory.create(Format.JSON, statementProps);
  }

  @Test
  public void shouldThrowOnDelimitedIfValuesAvroSchemaNameSet() {
    // Given:
    when(sinkProps.get(DdlConfig.VALUE_AVRO_SCHEMA_FULL_NAME))
        .thenReturn(new StringLiteral("bob"));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("VALUE_AVRO_SCHEMA_FULL_NAME is only valid for AVRO topics.");

    // When:
    factory.create(Format.DELIMITED, sinkProps);
  }

  @Test
  public void shouldPickUpValueAvroSchemaNameFromStatementProps() {
    // Given:
    when(statementProps.getValueAvroSchemaName()).thenReturn(Optional.of("vic"));

    // When:
    final KsqlSerdeFactory result = factory.create(Format.AVRO, statementProps);

    // Then:
    assertThat(result, is(new KsqlAvroSerdeFactory("vic")));
  }

  @Test
  public void shouldPickUpValueAvroSchemaNameFromSinkProps() {
    // Given:
    when(sinkProps.get(DdlConfig.VALUE_AVRO_SCHEMA_FULL_NAME))
        .thenReturn(new StringLiteral("hojjat"));

    // When:
    final KsqlSerdeFactory result = factory.create(Format.AVRO, sinkProps);

    // Then:
    assertThat(result, is(new KsqlAvroSerdeFactory("hojjat")));
  }

  @Test
  public void shouldPickUpDefaultValueAvroSchemaName() {
    // When:
    final KsqlSerdeFactory result = factory.create(Format.AVRO, sinkProps);

    // Then:
    assertThat(result, is(new KsqlAvroSerdeFactory(KsqlConstants.DEFAULT_AVRO_SCHEMA_FULL_NAME)));
  }
}