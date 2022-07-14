/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.util;

import io.confluent.ksql.api.util.ApiServerUtils;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

public final class CommandStoreUtil {

  private CommandStoreUtil() {
  }

  public static void httpWaitForCommandSequenceNumber(
      final CommandQueue commandQueue,
      final KsqlRequest request,
      final Duration timeout
  ) {
    try {
      waitForCommandSequenceNumber(commandQueue, request, timeout);
    } catch (final InterruptedException e) {
      ApiServerUtils.setMaskedSqlIfNeeded(request);
      final long seqNum = request.getCommandSequenceNumber().orElse(-1L);
      final String errorMsg = "Interrupted while waiting for command with the supplied "
          + "sequence number to execute. sequence number: " + seqNum
          + ", request: " + request.getMaskedKsql();
      throw new KsqlRestException(
          Errors.serverErrorForStatement(e, errorMsg, new KsqlEntityList()));
    } catch (final TimeoutException e) {
      throw new KsqlRestException(Errors.commandQueueCatchUpTimeout(
          request.getCommandSequenceNumber().orElse(-1L)));
    }
  }

  public static void waitForCommandSequenceNumber(
      final CommandQueue commandQueue,
      final KsqlRequest request,
      final Duration timeout
  ) throws InterruptedException, TimeoutException {
    final Optional<Long> commandSequenceNumber = request.getCommandSequenceNumber();
    if (commandSequenceNumber.isPresent()) {
      final long seqNum = commandSequenceNumber.get();
      commandQueue.ensureConsumedPast(seqNum, timeout);
    }
  }
}
