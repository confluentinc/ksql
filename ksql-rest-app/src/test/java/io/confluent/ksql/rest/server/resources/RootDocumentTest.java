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
 */

package io.confluent.ksql.rest.server.resources;

import org.eclipse.jetty.http.HttpStatus;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

/**
 * @author andy
 * created 17/04/2018
 */
public class RootDocumentTest {

  @Test
  public void shouldRedirectToInfoIfNoUI() throws Exception {
    // Given:
    final RootDocument doc = new RootDocument();
    final UriInfo uriInfo = uriInfo("http://some/proxy");

    // When:
    final Response response = doc.get(uriInfo);

    // Then:
    assertThat(response.getStatus(), is(HttpStatus.TEMPORARY_REDIRECT_307));
    assertThat(response.getLocation().toString(), is("http://some/proxy/info"));
  }

  private UriInfo uriInfo(final String uri) throws URISyntaxException {
    final UriInfo uriInfo = mock(UriInfo.class);
    expect(uriInfo.getAbsolutePath()).andReturn(new URI(uri));
    replay(uriInfo);
    return uriInfo;
  }
}