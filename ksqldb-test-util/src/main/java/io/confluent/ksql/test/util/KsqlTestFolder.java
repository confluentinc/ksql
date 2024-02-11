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

package io.confluent.ksql.test.util;

import java.io.File;
import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.junit.rules.TemporaryFolder;

/**
 * A Utility class to consolidate where we allocate state folders for tests.
 */
public final class KsqlTestFolder {

  /**
   * Uninstantiable static utility class
   */
  private KsqlTestFolder() { }


  /**
   * Create a temporary folder for testing. Rather than the system default
   * temp directory, use the current working directory to avoid directories
   * being deleted during tests.
   */
  public static TemporaryFolder temporaryFolder() {
    final TemporaryFolder temporaryFolder = TemporaryFolder
        .builder()
        .parentFolder(new File(System.getProperty("user.dir")))
        .build();

    // Just in case the JVM exits without a proper test failure.
    // Using a weak reference to avoid keeping all the temp folder references around
    // for the whole duration of the JVM.
    final WeakReference<TemporaryFolder> reference = new WeakReference<>(temporaryFolder);
    final Thread hook = new Thread(() -> {
      final TemporaryFolder folder = reference.get();
      if (folder != null) {
        folder.delete();
      }
    });

    // I wish we had a way to intercept deletes to unregister these hooks.
    // As it is, as tests complete, the folders themselves will get deleted,
    // and the TemporaryFolder instances will get GCed after the tests, but the
    // Thread instances will live in the Runtime until the end of the build.
    // It shouldn't cause a problem, but in case it does, I think the next step would
    // be to implement ExternalResource here and remove these references in the after()
    // method.
    Runtime.getRuntime().addShutdownHook(hook);

    return temporaryFolder;
  }

  public static void startWatching(final TemporaryFolder folder, final boolean isWithFileWatcher,
                             final AtomicBoolean isWatcherRunning) {
    if (isWithFileWatcher) {
      try {
        final FileAlterationObserver fileAlterationObserver =
            new FileAlterationObserver(folder.getRoot());
//        final FileAlterationObserver fileAlterationObserver =
//            new FileAlterationObserver(System.getProperty("user.dir"));
        fileAlterationObserver.addListener(new FileAlterationListener() {
          @Override
          public void onStart(final FileAlterationObserver fileAlterationObserver) {

          }

          @Override
          public void onDirectoryCreate(final File file) {
            if (file.getAbsolutePath().contains("_confluent-command-0")) {
              System.out.println("Checkthis");
            }
            System.out.printf("##### Directory created - %s. can-read - %b, can-write - %b, "
                    + "can-execute - %b, Current thread id - %d, Current thread name - %s",
                file.getAbsolutePath(),
                file.canRead(),
                file.canWrite(),
                file.canExecute(),
                Thread.currentThread().getId(),
                Thread.currentThread().getName()
            );
            System.out.println("-");
          }

          @Override
          public void onDirectoryChange(final File file) {
            if (file.getAbsolutePath().contains("_confluent-command-0")) {
              System.out.println("Checkthis");
            }
            System.out.printf("##### Directory changed - %s. can-read - %b, can-write - %b, "
                    + "can-execute - %b, Current thread id - %d, Current thread name - %s",
                file.getAbsolutePath(),
                file.canRead(),
                file.canWrite(),
                file.canExecute(),
                Thread.currentThread().getId(),
                Thread.currentThread().getName()
            );
            System.out.println("-");
          }

          @Override
          public void onDirectoryDelete(final File file) {
            if (file.getAbsolutePath().contains("_confluent-command-0")) {
              System.out.println("Checkthis");
            }
            System.out.printf("##### Directory deleted - %s. can-read - %b, can-write - %b, "
                    + "can-execute - %b, Current thread id - %d, Current thread name - %s",
                file.getAbsolutePath(),
                file.canRead(),
                file.canWrite(),
                file.canExecute(),
                Thread.currentThread().getId(),
                Thread.currentThread().getName()
            );
            System.out.println("-");
          }

          @Override
          public void onFileCreate(final File file) {
            if (file.getAbsolutePath().contains("_confluent-command-0")) {
              System.out.println("Checkthis");
            }
            System.out.printf("##### File created - %s. can-read - %b, can-write - %b, "
                    + "can-execute - %b, Current thread id - %d, Current thread name - %s",
                file.getAbsolutePath(),
                file.canRead(),
                file.canWrite(),
                file.canExecute(),
                Thread.currentThread().getId(),
                Thread.currentThread().getName()
            );
            System.out.println("-");
          }

          @Override
          public void onFileChange(final File file) {
            if (file.getAbsolutePath().contains("_confluent-command-0")) {
              System.out.println("Checkthis");
            }
            System.out.printf("##### File changed - %s. can-read - %b, can-write - %b, "
                    + "can-execute - %b, Current thread id - %d, Current thread name - %s",
                file.getAbsolutePath(),
                file.canRead(),
                file.canWrite(),
                file.canExecute(),
                Thread.currentThread().getId(),
                Thread.currentThread().getName()
            );
            System.out.println("-");
          }

          @Override
          public void onFileDelete(final File file) {
            if (file.getAbsolutePath().contains("_confluent-command-0")) {
              System.out.println("Checkthis");
            }
            System.out.printf("##### File deleted - %s. can-read - %b, can-write - %b, "
                    + "can-execute - %b, Current thread id - %d, Current thread name - %s",
                file.getAbsolutePath(),
                file.canRead(),
                file.canWrite(),
                file.canExecute(),
                Thread.currentThread().getId(),
                Thread.currentThread().getName()
            );
            System.out.println("-");
          }

          @Override
          public void onStop(final FileAlterationObserver fileAlterationObserver) {

          }
        });
        final FileAlterationObserverRunner runner =
            new FileAlterationObserverRunner(fileAlterationObserver, isWatcherRunning);
        final Thread fileObs = new Thread(runner);
        fileObs.start();
        System.out.printf(
            "Started new thread id: %d, name: %s",
            Thread.currentThread().getId(),
            Thread.currentThread().getName()
        );
        System.out.println("-");
      } catch (Exception e) {
        System.out.println("Error while starting file alteration observer." + e.getMessage());
      }
    }
  }
}
