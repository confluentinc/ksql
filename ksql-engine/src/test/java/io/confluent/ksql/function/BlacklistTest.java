/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.function;

import com.google.common.collect.ImmutableList;

import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class BlacklistTest {

  private File blacklistFile;

  @Before
  public void before() throws IOException {
     blacklistFile = TestUtils.tempFile();
  }

  @Test
  public void shouldBlackListAllInPackage() throws IOException {
    writeBlacklist(ImmutableList.of("java.lang"));
    final Blacklist blacklist = new Blacklist(this.blacklistFile);
    assertTrue(blacklist.test("java.lang.Class"));
    assertFalse(blacklist.test("java.util.List"));
  }

  @Test
  public void shouldBlackListClassesMatching() throws IOException {
    writeBlacklist(ImmutableList.of("java.lang.Process"));
    final Blacklist blacklist = new Blacklist(this.blacklistFile);
    assertTrue(blacklist.test("java.lang.Process"));
    assertTrue(blacklist.test("java.lang.ProcessBuilder"));
    assertTrue(blacklist.test("java.lang.ProcessEnvironment"));
    assertFalse(blacklist.test("java.lang.Class"));
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void shouldNotBlacklistAnythingIfFailsToLoadFile() {
    blacklistFile.delete();
    final Blacklist blacklist = new Blacklist(this.blacklistFile);
    assertFalse(blacklist.test("java.lang.Process"));
    assertFalse(blacklist.test("java.util.List"));
    assertFalse(blacklist.test("java.lang.ProcessEnvironment"));
    assertFalse(blacklist.test("java.lang.Class"));
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void shouldNotBlacklistAnythingIfBlacklistFileIsEmpty() {
    final Blacklist blacklist = new Blacklist(this.blacklistFile);
    assertFalse(blacklist.test("java.lang.Process"));
    assertFalse(blacklist.test("java.util.List"));
    assertFalse(blacklist.test("java.lang.ProcessEnvironment"));
    assertFalse(blacklist.test("java.lang.Class"));
  }

  @Test
  public void shouldIgnoreBlankLines() throws IOException {
    writeBlacklist(ImmutableList.<String>builder().add("", "java.util", "").build());
    final Blacklist blacklist = new Blacklist(this.blacklistFile);
    assertFalse(blacklist.test("java.lang.Process"));
    assertTrue(blacklist.test("java.util.List"));
  }

  @Test
  public void shouldIgnoreLinesStartingWithHash() throws IOException {
    writeBlacklist(ImmutableList.<String>builder().add("#", "java.util", "#").build());
    final Blacklist blacklist = new Blacklist(this.blacklistFile);
    assertFalse(blacklist.test("java.lang.String"));
    assertTrue(blacklist.test("java.util.Map"));
  }

  @Test
  public void shouldNotBlackListAllClassesIfItemEndsWith$() throws IOException {
    writeBlacklist(ImmutableList.<String>builder().add("java.lang.Runtime$").build());
    final Blacklist blacklist = new Blacklist(this.blacklistFile);
    assertTrue(blacklist.test("java.lang.Runtime"));
    assertFalse(blacklist.test("java.lang.RuntimeException"));
  }

  private void writeBlacklist(final List<String> blacklisted) throws IOException {
    Files.write(blacklistFile.toPath(), blacklisted, StandardCharsets.UTF_8);
  }

}