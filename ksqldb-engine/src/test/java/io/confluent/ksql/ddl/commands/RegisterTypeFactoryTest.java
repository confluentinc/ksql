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

package io.confluent.ksql.ddl.commands;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import io.confluent.ksql.execution.ddl.commands.RegisterTypeCommand;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.RegisterType;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RegisterTypeFactoryTest {
  private static final String EXISTING_TYPE = "existing_type";
  private static final String NOT_EXISTING_TYPE = "not_existing_type";
  private RegisterTypeFactory factory;

  @Mock
  private MetaStore metaStore;
  @Mock
  private SqlType customType;

  @Before
  public void setUp() {
    when(metaStore.resolveType(EXISTING_TYPE)).thenReturn(Optional.of(customType));
    factory = new RegisterTypeFactory(metaStore);
  }

  @Test
  public void shouldCreateCommandForRegisterTypeWhenIfNotExitsSet() {
    // Given:
    final RegisterType ddlStatement = new RegisterType(
        Optional.empty(),
        NOT_EXISTING_TYPE,
        new Type(SqlStruct.builder().field("foo", SqlPrimitiveType.of(SqlBaseType.STRING)).build()),
        true
    );

    // When:
    final RegisterTypeCommand result = factory.create(ddlStatement);

    // Then:
    assertThat(result.getType(), equalTo(ddlStatement.getType().getSqlType()));
    assertThat(result.getTypeName(), equalTo(NOT_EXISTING_TYPE));
  }

  @Test
  public void shouldCreateCommandForRegisterTypeWhenIfNotExitsNotSet() {
    // Given:
    final RegisterType ddlStatement = new RegisterType(
        Optional.empty(),
        NOT_EXISTING_TYPE,
        new Type(SqlStruct.builder().field("foo", SqlPrimitiveType.of(SqlBaseType.STRING)).build()),
        false
    );

    // When:
    final RegisterTypeCommand result = factory.create(ddlStatement);

    // Then:
    assertThat(result.getType(), equalTo(ddlStatement.getType().getSqlType()));
    assertThat(result.getTypeName(), equalTo(NOT_EXISTING_TYPE));
  }

  @Test
  public void shouldNotThrowOnRegisterExistingTypeWhenIfNotExistsSet() {
    // Given:
    final RegisterType ddlStatement = new RegisterType(
        Optional.empty(),
        EXISTING_TYPE,
        new Type(SqlStruct.builder().field("foo", SqlPrimitiveType.of(SqlBaseType.STRING)).build()),
        true
    );

    // When:
    final RegisterTypeCommand result = factory.create(ddlStatement);

    // Then:
    assertThat(result.getType(), equalTo(ddlStatement.getType().getSqlType()));
    assertThat(result.getTypeName(), equalTo(EXISTING_TYPE));
  }

  @Test
  public void shouldThrowOnRegisterExistingTypeWhenIfNotExistsNotSet() {
    // Given:
    final RegisterType ddlStatement = new RegisterType(
        Optional.empty(),
        EXISTING_TYPE,
        new Type(SqlStruct.builder().field("foo", SqlPrimitiveType.of(SqlBaseType.STRING)).build()),
        false
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> factory.create(ddlStatement)
    );

    // Then:
    assertThat(
        e.getMessage(),
        equalTo("Cannot register custom type '"
            + EXISTING_TYPE + "' since it is already registered with type: " + customType)
    );
  }
}
