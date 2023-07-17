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

public class MigrationsDirectoryUtilTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private String testDir;

  @Before
  public void setUp() {
    testDir = folder.getRoot().getPath();
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

  private void givenFileWithContents(final String filename, final String contents) throws Exception {
    assertThat(new File(filename).createNewFile(), is(true));

    try (PrintWriter out = new PrintWriter(filename, Charset.defaultCharset().name())) {
      out.println(contents);
    } catch (FileNotFoundException | UnsupportedEncodingException e) {
      Assert.fail("Failed to write test file: " + filename);
    }
  }
}