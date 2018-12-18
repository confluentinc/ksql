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

package io.confluent.ksql.version.metrics;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.junit.Test;
import org.slf4j.Logger;

public class KsqlVersionCheckerResponseHandlerTest {

  @Test
  public void testHandle() throws IOException {
    final HttpResponse response = mock(HttpResponse.class);
    final StatusLine statusLine = mock(StatusLine.class);
    final HttpEntity entity = mock(HttpEntity.class);
    final Logger log = mock(Logger.class);
    final Header header = mock(Header.class);
    expect(response.getStatusLine()).andReturn(statusLine).once();
    expect(statusLine.getStatusCode()).andReturn(HttpStatus.SC_OK).once();
    expect(response.getEntity()).andReturn(entity).times(2);
    final ByteArrayInputStream bais = new ByteArrayInputStream("yolo".getBytes(StandardCharsets.UTF_8));
    expect(entity.getContent()).andReturn(bais).times(2);
    expect(entity.getContentType()).andReturn(header).times(1);
    expect(header.getElements()).andReturn(new HeaderElement[]{});
    expect(entity.getContentLength()).andReturn(4L).times(2);
    log.warn("yolo");
    expectLastCall().once();
    replay(response, statusLine, entity, header, log);
    final KsqlVersionCheckerResponseHandler kvcr = new KsqlVersionCheckerResponseHandler(log);
    kvcr.handle(response);
    verify(response, statusLine, entity, header, log);
  }
}
