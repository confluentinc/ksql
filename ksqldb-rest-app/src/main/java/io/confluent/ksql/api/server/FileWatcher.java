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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchService;
import java.nio.file.WatchKey;
import java.nio.file.WatchEvent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

// reference https://gist.github.com/danielflower/f54c2fe42d32356301c68860a4ab21ed
public class FileWatcher implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(FileWatcher.class);
  private static final ExecutorService executor = Executors.newFixedThreadPool(1,
      new ThreadFactory() {
        public Thread newThread(Runnable r) {
          final Thread t = Executors.defaultThreadFactory().newThread(r);
          t.setDaemon(true);
          return t;
        }
      });

  public interface Callback {
    void run() throws Exception;
  }

  private volatile boolean shutdown;
  private final WatchService watchService;
  private final Path file;
  private final Callback callback;

  public FileWatcher(final Path file, final Callback callback) throws IOException {
    this.file = file;
    this.watchService = FileSystems.getDefault().newWatchService();
    // Listen to both CREATE and MODIFY to reload, which handles delete then create.
    file.getParent().register(watchService,
        StandardWatchEventKinds.ENTRY_CREATE,
        StandardWatchEventKinds.ENTRY_MODIFY);
    this.callback = callback;
  }

  /**
   * Starts watching a file and calls the callback when it is changed.
   */
  public static void onFileChange(final Path file, final Callback callback) throws IOException {
    log.info("Configuring file watcher to watch for changes: " + file);
    final FileWatcher fileWatcher = new FileWatcher(file, callback);
    executor.submit(fileWatcher);
  }

  public void run() {
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
    }
  }

  @SuppressWarnings("unchecked")
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
      final Path changed = this.file.getParent().resolve(ev.context());
      log.debug("Watch file change: " + ev.context() + "=>" + changed);
      // use Path.equals rather than Files.isSameFile to handle updated symlinks
      if (Files.exists(changed) && changed.equals(this.file)) {
        log.info("Change event for watched file: " + file);
        try {
          callback.run();
        } catch (Exception e) {
          log.warn("Hit error callback on file change", e);
        }
        break;
      }
    }
    key.reset();
  }

  public void shutdown() {
    shutdown = true;
    try {
      watchService.close();
    } catch (IOException e) {
      log.info("Error closing watch service", e);
    }
  }

}