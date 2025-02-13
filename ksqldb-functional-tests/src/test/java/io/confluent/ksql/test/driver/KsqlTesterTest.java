/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.test.driver;

import io.confluent.ksql.tools.test.parser.SqlTestLoader;
import io.confluent.ksql.tools.test.parser.SqlTestLoader.SqlTest;
import io.confluent.ksql.tools.test.SqlTestExecutor;
import io.confluent.ksql.test.util.KsqlTestFolder;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class KsqlTesterTest {
  private static final String TEST_DIR = "/sql-tests";

  // parameterized
  private SqlTest test;

  @Rule
  public final TemporaryFolder tmpFolder = KsqlTestFolder.temporaryFolder();
  private SqlTestExecutor executor;

  @SuppressWarnings("unused")
  public KsqlTesterTest(final String testCase, final SqlTest test) {
    this.test = test;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Object[][] data() throws IOException {
    final Path testDir = Paths.get(KsqlTesterTest.class.getResource(TEST_DIR).getFile());
    final SqlTestLoader loader = new SqlTestLoader(testDir);
    return loader.load()
        .map(test -> new Object[]{
        "(" + test.getFile().getParent().toFile().getName()
            + "/" + test.getFile().toFile().getName() + ") "
            + test.getName(), test})
        .toArray(Object[][]::new);
  }

  @After
  public void close() {
    executor.close();
  }

  @Before
  public void setUp() {
    this.executor = SqlTestExecutor.create(tmpFolder.getRoot().toPath());
  }

  @Test
  public void test() {
    executor.executeTest(test);
  }
}
