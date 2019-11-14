/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.rest.entity.StreamedRow;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.Response;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.StringUtils;

public final class QueryStream implements Closeable, Iterator<StreamedRow> {

  static final int READ_TIMEOUT_MS = (int) TimeUnit.SECONDS.toMillis(2);

  private final Response response;
  private final ObjectMapper objectMapper;
  private final Scanner responseScanner;
  private final InputStreamReader isr;

  private StreamedRow bufferedRow;
  private volatile boolean closed = false;

  QueryStream(final Response response) {
    this.response = response;

    this.objectMapper = JsonMapper.INSTANCE.mapper;
    this.isr = new InputStreamReader(
        (InputStream) response.getEntity(),
        StandardCharsets.UTF_8
    );
    this.responseScanner = new Scanner((buf) -> {
      while (true) {
        try {
          return isr.read(buf);
        } catch (final SocketTimeoutException e) {
          // Read timeout:
          if (closed) {
            return -1;
          }
        } catch (final IOException e) {
          // Can occur if isr closed:
          if (closed) {
            return -1;
          }

          throw e;
        }
      }
    });

    this.bufferedRow = null;
  }

  @Override
  public boolean hasNext() {
    if (bufferedRow != null) {
      return true;
    }

    return bufferNextRow();
  }

  @Override
  public StreamedRow next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    final StreamedRow result = bufferedRow;
    bufferedRow = null;
    return result;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }

    synchronized (this) {
      closed = true;
    }
    responseScanner.close();
    response.close();
    IOUtils.closeQuietly(isr);
  }

  private boolean bufferNextRow() {
    try {
      while (responseScanner.hasNextLine()) {
        final String responseLine = responseScanner.nextLine().trim();

        final String jsonMsg = toJsonMsg(responseLine);

        if (jsonMsg.isEmpty()) {
          continue;
        }

        try {
          bufferedRow = objectMapper.readValue(jsonMsg, StreamedRow.class);
        } catch (final IOException exception) {
          if (closed) {
            return false;
          }
          throw new RuntimeException(exception);
        }
        return true;
      }

      return false;
    } catch (final IllegalStateException e) {
      // Can happen is scanner is closed:
      if (closed) {
        return false;
      }

      throw e;
    }
  }

  /**
   * Convert the single line within the full response into a valid JSON object.
   *
   * <p>The entire response is an array of JSON objects, e.g. in the form:
   *
   * <pre>
   *   {@code
   *   [{...stuff...},
   *    {...stuff...},
   *    ...more rows....
   *    {...stuff...}],
   *   }
   * </pre>
   *
   * <p>This method trims any leading {@code [} or trailing {@code ,} or {@code ]}
   */
  private static String toJsonMsg(final String responseLine) {
    String result = StringUtils.removeStart(responseLine, "[");
    result = StringUtils.removeEnd(result, "]");
    result = StringUtils.removeEnd(result, ",");
    return result.trim();
  }
}
