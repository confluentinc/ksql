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
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.DropType;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DropTypeCommandTest {

  private static final DropTypeCommand DROP =
      new DropTypeCommand(new DropType(Optional.empty(), "type"));

  @Mock
  private MutableMetaStore metaStore;

  @Test
  public void shouldDropExistingType() {
    // Given:
    when(metaStore.deleteType("type")).thenReturn(true);

    // When:
    final DdlCommandResult result = DROP.run(metaStore);

    // Then:
    verify(metaStore).deleteType("type");
    assertThat("Expected successful execution", result.isSuccess());
    assertThat(result.getMessage(), is("Dropped type 'type'"));
  }

  @Test
  public void shouldDropMissingType() {
    // Given:
    final DropTypeCommand command = new DropTypeCommand(new DropType(Optional.empty(), "type"));
    when(metaStore.deleteType("type")).thenReturn(false);

    // When:
    final DdlCommandResult result = command.run(metaStore);

    // Then:
    verify(metaStore).deleteType("type");
    assertThat("Expected successful execution", result.isSuccess());
    assertThat(result.getMessage(), is("Type 'type' does not exist"));
  }

}