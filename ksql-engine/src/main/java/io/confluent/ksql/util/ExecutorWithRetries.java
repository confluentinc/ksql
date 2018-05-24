/*
 * Copyright 2017 Confluent Inc.
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
 **/

package io.confluent.ksql.util;

import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public class ExecutorWithRetries {

  private static final int NUM_RETRIES = 5;
  private static final int RETRY_BACKOFF_MS = 500;
  private static final Logger log = LoggerFactory.getLogger(ExecutorWithRetries.class);

  public static <T> T execute(final Supplier<? extends Future<T>> supplier) throws Exception {
    int retries = 0;
    Exception lastException = null;
    while (retries < NUM_RETRIES) {
      try {
        if (retries != 0) {
          Thread.sleep(RETRY_BACKOFF_MS);
        }
        return supplier.get().get();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof RetriableException) {
          retries++;
          log.info("Retrying request due to retriable exception. Retry no: " + retries, e);
          lastException = e;
        } else if (e.getCause() instanceof Exception) {
          throw (Exception) e.getCause();
        } else {
          throw e;
        }
      }
    }
    throw lastException;
  }

  public static void execute(final Callable<Void> callable, final String errorMessage) {
    int retries = 0;
    while (retries < NUM_RETRIES) {
      try {
        if (retries != 0) {
          Thread.sleep(RETRY_BACKOFF_MS);
        }
        callable.call();
        break;
      } catch (Exception e) {
        retries++;
      } finally {
        if (retries == NUM_RETRIES) {
          throw new KsqlException(errorMessage);
        }
      }
    }
  }
}
