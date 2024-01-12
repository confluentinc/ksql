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

package io.confluent.ksql.version.metrics;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpStatus;
import org.easymock.internal.LastControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

public class KsqlVersionCheckerResponseHandlerTest {

  @Before
  public void before(){
    LastControl.pullMatchers();
  }

  @After
  public void after(){
    LastControl.pullMatchers();
  }

  @Test
  public void testHandle() throws IOException {
    // Given
    final ClassicHttpResponse response = mock(ClassicHttpResponse.class);
    final HttpEntity entity = mock(HttpEntity.class);
    final Logger log = mock(Logger.class);
    expect(response.getCode()).andReturn(HttpStatus.SC_OK).once();
    expect(response.getEntity()).andReturn(entity).times(2);
    final ByteArrayInputStream bais = new ByteArrayInputStream("yolo".getBytes(StandardCharsets.UTF_8));
    expect(entity.getContent()).andReturn(bais).times(2);
    expect(entity.getContentType()).andReturn("content-type").times(1);
    expect(entity.getContentLength()).andReturn(4L).times(1);
    log.warn("yolo");
    expectLastCall().once();
    replay(response, entity, log);

    final KsqlVersionCheckerResponseHandler kvcr = new KsqlVersionCheckerResponseHandler(log);

    // When
    kvcr.handle(response);

    // Then
    verify(response, entity, log);
  }
}
