/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.ddl.commands;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;

import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.tree.RegisterType;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RegisterTypeCommandTest {

  private static final String CUSTOM_NAME = "name";
  private static final Type TYPE = new Type(
      SqlStruct.builder()
          .field("foo", SqlPrimitiveType.of(SqlBaseType.STRING))
          .build());

  @Mock
  private MutableMetaStore metaStore;

  private RegisterTypeCommand command = new RegisterTypeCommand(
      new RegisterType(Optional.empty(), CUSTOM_NAME, TYPE));

  @Test
  public void shouldRegisterType() {
    // When:
    final DdlCommandResult result = command.run(metaStore);

    // Then:
    verify(metaStore).registerType(
        CUSTOM_NAME,
        TYPE.getSqlType());
    assertThat("Expected successful execution", result.isSuccess());
    assertThat(result.getMessage(), is("Registered custom type with name 'name' and SQL type STRUCT<`foo` STRING>"));
  }

}