/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.tools.test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.test.util.KsqlTestFolder;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SqlTestingToolTest {

  private final static String TESTS_FOLDER = "src/test/resources/sql-test-runner";
  @Rule
  public final TemporaryFolder tmpFolder = KsqlTestFolder.temporaryFolder();

  @Test
  public void shouldRunTests() throws IOException {
    final int result = SqlTestingTool.run(new String[]{"-td", TESTS_FOLDER, "-tf", tmpFolder.getRoot().getPath()});
    assertThat(result, is(0));
  }

  @Test
  public void shouldThrowOnInvalidArguments() throws IOException {
    final int result = SqlTestingTool.run(new String[]{"-abc"});
    assertThat(result, is(1));
  }
}
