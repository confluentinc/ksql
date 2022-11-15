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

import io.confluent.ksql.execution.ddl.commands.DropTypeCommand;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.DropType;
import java.util.Optional;

import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DropTypeFactoryTest {
  private static final String EXISTING_TYPE = "existing_type";
  private static final String NOT_EXISTING_TYPE = "not_existing_type";

  private DropTypeFactory factory;

  @Mock
  private MetaStore metaStore;
  @Mock
  private SqlType customType;

  @Before
  public void setUp() {
    when(metaStore.resolveType(EXISTING_TYPE)).thenReturn(Optional.of(customType));

    factory = new DropTypeFactory(metaStore);
  }

  @Test
  public void shouldCreateDropType() {
    // Given:
    final DropType dropType = new DropType(Optional.empty(), EXISTING_TYPE, false);

    // When:
    final DropTypeCommand cmd = factory.create(dropType);

    // Then:
    assertThat(cmd.getTypeName(), equalTo(EXISTING_TYPE));
  }

  @Test
  public void shouldCreateDropTypeForExistingTypeAndIfExistsSet() {
    // Given:
    final DropType dropType = new DropType(Optional.empty(), EXISTING_TYPE, true);

    // When:
    final DropTypeCommand cmd = factory.create(dropType);

    // Then:
    assertThat(cmd.getTypeName(), equalTo(EXISTING_TYPE));
  }

  @Test
  public void shouldNotFailCreateTypeIfTypeDoesNotExistAndIfExistsSet () {
    // Given:
    final DropType dropType = new DropType(Optional.empty(), NOT_EXISTING_TYPE, true);

    // When:
    final DropTypeCommand cmd = factory.create(dropType);

    // Then:
    assertThat(cmd.getTypeName(), equalTo(NOT_EXISTING_TYPE));
  }

  @Test
  public void shouldFailCreateTypeIfTypeDoesNotExist() {
    // Given:
    final DropType dropType = new DropType(Optional.empty(), NOT_EXISTING_TYPE, false);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> factory.create(dropType)
    );

    // Then:
    assertThat(e.getMessage(), equalTo("Type " + NOT_EXISTING_TYPE + " does not exist."));
  }
}
