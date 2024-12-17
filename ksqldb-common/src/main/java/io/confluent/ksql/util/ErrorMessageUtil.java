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

package io.confluent.ksql.util;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.text.WordUtils;

public final class ErrorMessageUtil {

  private static final String PREFIX = "Caused by: ";

  private ErrorMessageUtil() {
  }

  /**
   * Build an error message containing the message of each throwable in the chain.
   *
   * <p>Throwable messages are separated by new lines.
   *
   * @param throwable the top level error.
   * @return the error message.
   */
  public static String buildErrorMessage(final Throwable throwable) {
    if (throwable == null) {
      return "";
    }

    final List<String> messages = dedup(getErrorMessages(throwable));

    final String msg = messages.remove(0);

    final String causeMsg = messages.stream()
        .filter(s -> !s.isEmpty())
        .map(cause -> WordUtils.wrap(PREFIX + cause, 80, "\n\t", true))
        .collect(Collectors.joining(System.lineSeparator()));

    return causeMsg.isEmpty() ? msg : msg + System.lineSeparator() + causeMsg;
  }

  /**
   * Build a list containing the error message for each throwable in the chain.
   *
   * @param e the top level error.
   * @return the list of error messages.
   */
  public static List<String> getErrorMessages(final Throwable e) {
    return getThrowables(e).stream()
        .map(ErrorMessageUtil::getErrorMessage)
        .collect(Collectors.toList());
  }

  private static String getErrorMessage(final Throwable e) {
    if (e instanceof ConnectException) {
      return "Could not connect to the server. "
          + "Please check the server details are correct and that the server is running.";
    } else {
      final String message;
      if (e instanceof KsqlStatementException) {
        message = ((KsqlStatementException) e).getUnloggedMessage();
      } else {
        message = e.getMessage();
      }
      return message == null ? e.toString() : message;
    }
  }

  private static List<Throwable> getThrowables(final Throwable e) {
    final List<Throwable> list = new ArrayList<>();
    Throwable cause = e;
    while (cause != null && !list.contains(cause)) {
      list.add(cause);
      cause = cause.getCause();
    }
    return list;
  }

  private static List<String> dedup(final List<String> messages) {
    final List<String> dedupedMessages = new ArrayList<>(new LinkedHashSet<>(messages));
    final String message = dedupedMessages.get(0);
    dedupedMessages.subList(1, dedupedMessages.size()).removeIf(message::contains);
    return dedupedMessages;
  }
}
