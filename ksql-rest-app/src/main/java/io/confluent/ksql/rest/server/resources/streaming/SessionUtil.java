/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.resources.streaming;

import java.nio.charset.StandardCharsets;
import javax.websocket.CloseReason;
import javax.websocket.CloseReason.CloseCodes;
import javax.websocket.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class SessionUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SessionUtil.class);

  // Close reason is limited to 123 bytes:
  private static final int MAX_REASON_LEN = 123;

  private static final int ECLIPSE_LEN = "...".getBytes(StandardCharsets.UTF_8).length;

  private SessionUtil() {
  }

  static void closeSilently(
      final Session session,
      final CloseCodes code,
      final String message) {
    try {
      session.close(new CloseReason(code, truncate(message)));
    } catch (final Exception e) {
      LOG.info("Exception caught closing session {}", session.getId(), e);
    }
  }

  private static String truncate(final String reason) {
    if (reason == null) {
      return "";
    }

    if (reason.getBytes(StandardCharsets.UTF_8).length <= MAX_REASON_LEN) {
      return reason;
    }

    String truncated = reason.substring(0, Math.min(reason.length(), MAX_REASON_LEN - ECLIPSE_LEN));
    while (truncated.getBytes(StandardCharsets.UTF_8).length > MAX_REASON_LEN - ECLIPSE_LEN) {
      truncated = truncated.substring(0, truncated.length() - 2);
    }
    return truncated + "...";
  }
}
