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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// reference:
// https://gist.github.com/danielflower/f54c2fe42d32356301c68860a4ab21ed
// https://github.com/confluentinc/rest-utils/blob/master/core/src/main/java/io/confluent/rest/FileWatcher.java
/**
 * Watches a file and calls a callback when it is changed. Only file creation and modification
 * are watched for; deletion has no effect.
 */
public class FileWatcher extends Thread {

  private static final Logger log = LoggerFactory.getLogger(FileWatcher.class);

  public interface Callback {
    void run() throws Exception;
  }

  private volatile boolean shutdown;
  private final WatchService watchService;
  private final Path file;
  private final Path dir;
  private final Callback callback;
  private final WatchKey key;

  @SuppressFBWarnings(
      value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE",
      justification = "Null check on file.getParent() is present"
  )
  public FileWatcher(final Path file, final Callback callback) throws IOException {
    this.file = Objects.requireNonNull(file);
    this.dir = Objects.requireNonNull(file.getParent(), "Watch location must have parent");
    this.watchService = FileSystems.getDefault().newWatchService();
    this.callback = Objects.requireNonNull(callback);

    // Listen to both CREATE and MODIFY to reload, which handles delete then create.
    this.key = dir.register(watchService,
        StandardWatchEventKinds.ENTRY_CREATE,
        StandardWatchEventKinds.ENTRY_MODIFY);
  }

  /**
   * Closes the file watcher
   */
  public void shutdown() {
    log.info("Stopping file watcher from watching for changes: " + file);
    shutdown = true;
  }

  @Override
  public void run() {
    log.info("Starting file watcher to watch for changes: " + file);
    try {
      while (!shutdown && key.isValid()) {
        try {
          handleNextWatchNotification();
        } catch (Exception e) {
          log.error("Watch service caught exception, will continue:" + e);
        }
      }
    } finally {
      log.info("Stopped watching for TLS cert changes");
      try {
        watchService.close();
      } catch (IOException e) {
        log.info("Error closing watch service", e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void handleNextWatchNotification() throws InterruptedException {
    final WatchKey key = watchService.poll(1, TimeUnit.SECONDS);
    if (key == null) {
      return;
    }

    for (WatchEvent<?> event : key.pollEvents()) {
      final WatchEvent.Kind<?> kind = event.kind();
      if (kind == StandardWatchEventKinds.OVERFLOW) {
        log.info("Watch event is OVERFLOW - may have missed cert change");
        continue;
      }

      final WatchEvent<Path> ev = (WatchEvent<Path>)event;
      final Path changed = dir.resolve(ev.context());
      log.debug("Watch file change: " + changed);

      // use Path.equals rather than Files.isSameFile to handle updated symlinks
      if (Files.exists(changed) && changed.equals(file)) {
        log.info("Change event for watched file: " + file);
        try {
          callback.run();
        } catch (Exception e) {
          log.error("Hit error callback on file change", e);
        }
        break;
      }
    }

    if (!key.reset()) {
      log.error("Watch reset failed. No longer watching directory. "
          + "This is likely because the directory was deleted or renamed. Path: " + dir);
    }
  }
}