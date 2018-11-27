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
 **/

package io.confluent.ksql.rest.util;

import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.computation.ReplayableCommandQueue;
import io.confluent.ksql.rest.server.resources.Errors;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

public final class CommandStoreUtil {
  private CommandStoreUtil() {
  }

  public static void httpWaitForCommandOffset(
      final ReplayableCommandQueue replayableCommandQueue,
      final KsqlRequest request,
      final long timeout) {
    try {
      waitForCommandOffset(replayableCommandQueue, request, timeout);
    } catch (final TimeoutException e) {
      throw new KsqlRestException(Errors.commandQueueCatchUpTimeout(e.getMessage()));
    }
  }

  public static void waitForCommandOffset(
      final ReplayableCommandQueue replayableCommandQueue,
      final KsqlRequest request,
      final long timeout) throws TimeoutException {
    final Optional<Long> commandOffset = request.getCommandOffset();
    if (commandOffset.isPresent()) {
      final long offset = commandOffset.get();
      replayableCommandQueue.ensureConsumedUpThrough(offset, timeout);
    }
  }
}
