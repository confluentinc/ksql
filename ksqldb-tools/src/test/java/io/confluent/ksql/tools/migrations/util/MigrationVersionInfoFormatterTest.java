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

package io.confluent.ksql.tools.migrations.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Before;
import org.junit.Test;

public class MigrationVersionInfoFormatterTest {

  private MigrationVersionInfoFormatter formatter;

  @Before
  public void setUp() {
    formatter = new MigrationVersionInfoFormatter();
  }

  @Test
  public void shouldFormatVersionInfo() {
    // Given:
    formatter.addVersionInfo(new MigrationVersionInfo(
        1, "hash", "<none>", "MIGRATED",
        "name", "N/A", "N/A", "N/A"));
    formatter.addVersionInfo(new MigrationVersionInfo(
        2, "other_hash", "N/A", "PENDING",
        "other_name", "N/A", "N/A", "N/A"));

    // When:
    final String formatted = formatter.getFormatted();

    // Then:
    assertThat(formatted, is(
        " Version | Name       | State    | Previous Version | Started On | Completed On | Error Reason \n" +
        "-----------------------------------------------------------------------------------------------\n" +
        " 1       | name       | MIGRATED | <none>           | N/A        | N/A          | N/A          \n" +
        " 2       | other_name | PENDING  | N/A              | N/A        | N/A          | N/A          \n" +
        "-----------------------------------------------------------------------------------------------\n"));
  }
}