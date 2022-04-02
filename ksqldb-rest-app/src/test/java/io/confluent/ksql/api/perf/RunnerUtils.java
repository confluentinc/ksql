/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.perf;

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.core.streams.WriteStream;
import java.util.List;

public class RunnerUtils {

  protected static final List<String> DEFAULT_COLUMN_NAMES = ImmutableList
      .of("name", "age", "male");
  protected static final List<String> DEFAULT_COLUMN_TYPES = ImmutableList
      .of("STRING", "INT", "BOOLEAN");
  protected static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("name"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("age"), SqlTypes.INTEGER)
      .keyColumn(ColumnName.of("male"), SqlTypes.BOOLEAN)
      .build();

  protected static final List<?> DEFAULT_KEY = ImmutableList.of("tim");

  protected static final GenericRow DEFAULT_ROW = GenericRow.genericRow("tim", 105, true);

  public static class ReceiveStream implements WriteStream<Buffer> {

    private RecordParser recordParser;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
    public ReceiveStream(final RecordParser recordParser) {
      this.recordParser = recordParser;
    }

    @Override
    public WriteStream<Buffer> exceptionHandler(final Handler<Throwable> handler) {
      return this;
    }

    @Override
    public Future<Void> write(final Buffer data) {
      write(data, null);
      return Future.succeededFuture();
    }

    @Override
    public void write(final Buffer data, final Handler<AsyncResult<Void>> handler) {
      recordParser.handle(data);
    }

    @Override
    public Future<Void> end() {
      return Future.succeededFuture();
    }

    @Override
    public void end(final Handler<AsyncResult<Void>> handler) {

    }

    @Override
    public WriteStream<Buffer> setWriteQueueMaxSize(final int maxSize) {
      return this;
    }

    @Override
    public boolean writeQueueFull() {
      return false;
    }

    @Override
    public WriteStream<Buffer> drainHandler(@Nullable final Handler<Void> handler) {
      return this;
    }
  }
}
