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

package io.confluent.ksql.api.server;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// reference:
// https://gist.github.com/danielflower/f54c2fe42d32356301c68860a4ab21ed
// https://github.com/confluentinc/rest-utils/blob/master/core/src/main/java/io/confluent/rest/FileWatcher.java
/**
 * Watches a file and calls a callback when it is changed.
 */
public class FileWatcher extends Thread {

  private static final Logger log = LoggerFactory.getLogger(FileWatcher.class);

  public interface Callback {
    void run() throws Exception;
  }

  private volatile boolean shutdown;
  private final WatchService watchService;
  private final Path file;
  private final Callback callback;

  @SuppressFBWarnings(
      value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE",
      justification = "Null check on file.getParent() is present"
  )
  public FileWatcher(final Path file, final Callback callback) throws IOException {
    this.file = Objects.requireNonNull(file);
    Objects.requireNonNull(file.getParent(), "Watch location must have parent");
    this.watchService = FileSystems.getDefault().newWatchService();
    // Listen to both CREATE and MODIFY to reload, which handles delete then create.
    file.getParent().register(watchService,
        StandardWatchEventKinds.ENTRY_CREATE,
        StandardWatchEventKinds.ENTRY_MODIFY);
    this.callback = Objects.requireNonNull(callback);
  }

  /**
   * Closes the file watcher
   */
  public void shutdown() {
    shutdown = true;
  }

  @Override
  public void run() {
    log.info("Starting file watcher to watch for changes: " + file);
    try {
      while (!shutdown) {
        try {
          handleNextWatchNotification();
        } catch (InterruptedException e) {
          throw e;
        } catch (Exception e) {
          log.info("Watch service caught exception, will continue:" + e);
        }
      }
    } catch (InterruptedException e) {
      log.info("Ending watch due to interrupt");
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
  @SuppressFBWarnings(
      value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE",
      justification = "Null check on file.getParent() is present above"
  )
  private void handleNextWatchNotification() throws InterruptedException {
    // wait for key to be signalled
    final WatchKey key = watchService.take();
    log.info("Watch Key notified");
    for (WatchEvent<?> event : key.pollEvents()) {
      final WatchEvent.Kind<?> kind = event.kind();
      if (kind == StandardWatchEventKinds.OVERFLOW) {
        log.debug("Watch event is OVERFLOW");
        continue;
      }
      final WatchEvent<Path> ev = (WatchEvent<Path>)event;
      final Path changed = file.getParent().resolve(ev.context());
      log.debug("Watch file change: " + ev.context() + "=>" + changed);
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
    key.reset();
  }

}