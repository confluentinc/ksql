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
import static org.mockito.Mockito.when;

import io.confluent.ksql.test.util.KsqlTestFolder;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MigrationsDirectoryUtilTest {

  private static final String CUSTOM_DIR = "/path/to/custom/dir";

  @Rule
  public TemporaryFolder folder = KsqlTestFolder.temporaryFolder();

  @Mock
  private MigrationConfig config;

  private String testDir;
  private String migrationsConfigPath;

  @Before
  public void setUp() {
    testDir = folder.getRoot().getPath();
    migrationsConfigPath = Paths.get(testDir, MigrationsDirectoryUtil.MIGRATIONS_CONFIG_FILE).toString();
  }

  @Test
  public void shouldComputeHashForFile() throws Exception {
    // Given:
    final String filePath = Paths.get(testDir, "test_file.txt").toString();
    givenFileWithContents(filePath, "some text");

    // When:
    final String hash = MigrationsDirectoryUtil.computeHashForFile(filePath);

    // Then:
    assertThat(hash, is("4d93d51945b88325c213640ef59fc50b"));
  }

  @Test
  public void shouldDefaultMigrationsDirBasedOnConfigPath() {
    // Given:
    when(config.getString(MigrationConfig.KSQL_MIGRATIONS_DIR_OVERRIDE)).thenReturn("");

    // When / Then:
    assertThat(MigrationsDirectoryUtil.getMigrationsDir(migrationsConfigPath, config),
        is(Paths.get(testDir, MigrationsDirectoryUtil.MIGRATIONS_DIR).toString()));
  }

  @Test
  public void shouldOverrideMigrationsDirFromConfig() {
    // Given:
    when(config.getString(MigrationConfig.KSQL_MIGRATIONS_DIR_OVERRIDE)).thenReturn(CUSTOM_DIR);

    // When / Then:
    assertThat(MigrationsDirectoryUtil.getMigrationsDir(migrationsConfigPath, config), is(CUSTOM_DIR));
  }

  private void givenFileWithContents(final String filename, final String contents) throws Exception {
    assertThat(new File(filename).createNewFile(), is(true));

    try (PrintWriter out = new PrintWriter(filename, Charset.defaultCharset().name())) {
      out.println(contents);
    } catch (FileNotFoundException | UnsupportedEncodingException e) {
      Assert.fail("Failed to write test file: " + filename);
    }
  }
}