/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.tools.migrations.commands;

import com.github.rvesse.airline.SingleCommand;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ValidateMigrationsCommandTest {

  private static final SingleCommand<ValidateMigrationsCommand> PARSER =
      SingleCommand.singleCommand(ValidateMigrationsCommand.class);

  @Mock
  private MigrationConfig config;
  @Mock
  private Client ksqlClient;

  private ValidateMigrationsCommand command;

  @Before
  public void setUp() {

  }

}