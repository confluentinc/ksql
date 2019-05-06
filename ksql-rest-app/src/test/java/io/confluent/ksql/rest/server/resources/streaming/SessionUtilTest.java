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

package io.confluent.ksql.rest.server.resources.streaming;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import javax.websocket.CloseReason;
import javax.websocket.CloseReason.CloseCodes;
import javax.websocket.Session;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SessionUtilTest {

  @Mock
  private Session session;
  @Captor
  private ArgumentCaptor<CloseReason> reasonCaptor;

  @Test
  public void shouldCloseQuietly() throws Exception {
    // Given:
    doThrow(new RuntimeException("Boom")).when(session).close(any(CloseReason.class));

    // When:
    SessionUtil.closeSilently(session, CloseCodes.CANNOT_ACCEPT, "reason");

    // Then:
    verify(session).close(any());
    // And exception swallowed.
  }

  @Test
  public void shouldNotTruncateShortReasons() throws Exception {
    // Given:
    final String reason = "some short reason";

    // When:
    SessionUtil.closeSilently(session, CloseCodes.CANNOT_ACCEPT, reason);

    // Then:
    verify(session).close(reasonCaptor.capture());
    assertThat(reasonCaptor.getValue().getReasonPhrase(), is(reason));
  }

  @Test
  public void shouldTruncateMessageLongerThanCloseReasonAllows() throws Exception {
    // Given:
    final String reason = "A long message that is longer than the maximum size that the "
        + "CloseReason class will allow-------------------------------------------------";
    assertThat("invalid test", reason.getBytes(UTF_8).length, greaterThan(123));

    // When:
    SessionUtil.closeSilently(session, CloseCodes.CANNOT_ACCEPT, reason);

    // Then:
    verify(session).close(reasonCaptor.capture());
    assertThat(reasonCaptor.getValue().getReasonPhrase(), is(
        "A long message that is longer than the maximum size that the CloseReason class "
            + "will allow-------------------------------..."));
    assertThat(reasonCaptor.getValue().getReasonPhrase().getBytes(UTF_8).length,
        is(123));
  }

  @Test
  public void shouldTruncateLongMessageWithMultiByteChars() throws Exception {
    // Given:
    final String reason = "A long message that is longer than the maximum size that the "
        + "CloseReason class will allow €€€€€€€€€€€€€...................................";
    assertThat("invalid test", reason.getBytes(UTF_8).length, greaterThan(123));

    // When:
    SessionUtil.closeSilently(session, CloseCodes.CANNOT_ACCEPT, reason);

    // Then:
    verify(session).close(reasonCaptor.capture());
    assertThat(reasonCaptor.getValue().getReasonPhrase(), is(
        "A long message that is longer than the maximum size that the CloseReason class will "
            + "allow €€€€€€€€€€..."));
  }

  @Test
  public void shouldHandleNullMessage() throws Exception {
    // When:
    SessionUtil.closeSilently(session, CloseCodes.CANNOT_ACCEPT, null);

    // Then:
    verify(session).close(reasonCaptor.capture());
    assertThat(reasonCaptor.getValue().getReasonPhrase(), is(""));
  }
}