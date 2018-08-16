/**
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

package io.confluent.ksql.rest.server.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.util.JsonMapper;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

@Path("/query")
@Produces(MediaType.APPLICATION_JSON)
public class MockStreamedQueryResource {
  List<TestStreamWriter> writers = new java.util.LinkedList<>();

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response streamQuery(final KsqlRequest request) throws Exception {
    final TestStreamWriter testStreamWriter = new TestStreamWriter();
    writers.add(testStreamWriter);
    return Response.ok().entity(testStreamWriter).build();
  }

  public List<TestStreamWriter> getWriters() { return writers; }

  public class TestStreamWriter implements StreamingOutput {
    BlockingQueue<String> dataq = new LinkedBlockingQueue<>();
    ObjectMapper objectMapper = JsonMapper.INSTANCE.mapper;

    public void enq(final String data) throws InterruptedException { dataq.put(data); }

    public void finished() throws InterruptedException { dataq.put(""); }

    private void writeRow(final String data, final OutputStream out) throws IOException {
      final List<Object> rowColumns = new java.util.LinkedList<Object>();
      rowColumns.add(data);
      final GenericRow row = new GenericRow(rowColumns);
      objectMapper.writeValue(out, StreamedRow.row(row));
      out.write("\n".getBytes(StandardCharsets.UTF_8));
      out.flush();
    }

    @Override
    public void write(final OutputStream out) throws IOException, WebApplicationException {
      out.write("\n".getBytes(StandardCharsets.UTF_8));
      out.flush();
      while (true) {
        final String data;
        try {
          data = dataq.take();
        } catch (final InterruptedException e) {
          throw new RuntimeException("take interrupted");
        }
        if (data.equals("")) {
          break;
        }
        writeRow(data, out);
      }
    }
  }
}
