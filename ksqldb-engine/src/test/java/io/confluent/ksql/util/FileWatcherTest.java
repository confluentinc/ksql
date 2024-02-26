/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.util;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import io.confluent.ksql.util.FileWatcher.Callback;
import io.confluent.ksql.test.util.KsqlTestFolder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.verification.Timeout;

@RunWith(MockitoJUnitRunner.class)
public class FileWatcherTest {

  @ClassRule
  public static final TemporaryFolder TMP = KsqlTestFolder.temporaryFolder();

  @Mock
  private Callback callback;
  public Path filePath;
  private FileWatcher watcher;
  private byte[] someBytes;

  @Before
  public void setUp() throws Exception {
    filePath = TMP.newFolder().toPath().resolve(UUID.randomUUID().toString());
    someBytes = "data".getBytes(UTF_8);
  }

  @After
  public void tearDown() {
    watcher.shutdown();
  }

  @Test
  public void shouldDetectFileCreated() throws Exception {
    // Given:
    watcher = new FileWatcher(filePath, callback);
    watcher.start();

    // When:
    Files.write(filePath, someBytes, StandardOpenOption.CREATE_NEW);

    // Then:
    verify(callback, new Timeout(TimeUnit.MINUTES.toMillis(1), atLeastOnce())).run();
  }

  @Test
  public void shouldDetectFileUpdated() throws Exception {
    // Given:
    Files.write(filePath, someBytes, StandardOpenOption.CREATE_NEW);

    waitForLastModifiedTick();

    watcher = new FileWatcher(filePath, callback);
    watcher.start();

    // When:
    Files.write(filePath, someBytes);

    // Then:
    // verify that this happens at least once, there's a possibility that the callback
    // gets called multiple times for one underlying change
    verify(callback, timeout(TimeUnit.MINUTES.toMillis(1)).atLeastOnce()).run();
  }

  @Test
  public void shouldShutdownAsync() throws Exception {
    watcher = new FileWatcher(filePath, callback);
    watcher.start();

    // When:
    watcher.shutdown();

    // Then:
    assertThatEventually(watcher::isAlive, is(false));
  }

  @Test
  public void willStopIfDirectoryDeleted() throws Exception {
    // Given:
    watcher = new FileWatcher(filePath, callback);
    watcher.start();

    // When:
    final Path parent = filePath.getParent();
    if (parent != null) {
      Files.delete(parent);
    } else {
      Assert.fail("Expected parent for " + filePath);
    }

    // Then:
    verify(callback, never()).run();
    assertThatEventually(watcher::isAlive, is(false));
  }

  /**
   * Resolution of {@link FileTime} on some OS / JDKs can have only second resolution. This can mean
   * the watcher 'misses' an update to a file that was <i>created</i> before the watcher was started
   * and <i>updated</i> after, if the update results in the same last modified time.
   *
   * <p>To ensure we stable test we must therefore wait for a second to ensure a different last
   * modified time.
   *
   * https://stackoverflow.com/questions/24804618/get-file-mtime-with-millisecond-resolution-from-java
   */
  private static void waitForLastModifiedTick() throws Exception {
    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
  }
}