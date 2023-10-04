/*
 * Copyright 2023 Confluent Inc.
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

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.core.streams.ReadStream;

@SuppressWarnings("unused")
public class SyncronizedRecordParser implements RecordParser {

  private static class WrappedUpstream implements ReadStream<Buffer> {

    private final ReadStream<Buffer> delegate;

    WrappedUpstream(final ReadStream<Buffer> delegate) {
      this.delegate = delegate;
    }

    @Override
    public ReadStream<Buffer> exceptionHandler(@Nullable final Handler<Throwable> handler) {
      return delegate.exceptionHandler(handler);
    }

    @Override
    @Fluent
    public ReadStream<Buffer> handler(@Nullable final Handler<Buffer> handler) {
      return delegate.handler(bf -> {
        synchronized (delegate) {
          handler.handle(bf);
        }
      });
    }

    @Override
    @Fluent
    public ReadStream<Buffer> pause() {
      return delegate.pause();
    }

    @Override
    @Fluent
    public ReadStream<Buffer> resume() {
      return delegate.resume();
    }

    @Override
    @Fluent
    public ReadStream<Buffer> fetch(final long l) {
      return delegate.fetch(l);
    }

    @Override
    @Fluent
    public ReadStream<Buffer> endHandler(@Nullable final Handler<Void> handler) {
      return delegate.endHandler(handler);
    }
  }

  private final RecordParser delegate;
  private final ReadStream<Buffer> source;

  public SyncronizedRecordParser(final RecordParser delegate, final ReadStream<Buffer> source) {
    this.delegate = delegate;
    this.source = source;
  }

  @Override
  public void setOutput(final Handler<Buffer> handler) {
    delegate.setOutput(handler);
  }

  public static RecordParser newDelimited(final String delim, final ReadStream<Buffer> stream) {
    return new SyncronizedRecordParser(
        RecordParser.newDelimited(delim, new WrappedUpstream(stream)),
        stream
    );
  }

  @Override
  public void delimitedMode(final String s) {
    delegate.delimitedMode(s);
  }

  @Override
  public void delimitedMode(final Buffer buffer) {
    delegate.delimitedMode(buffer);
  }

  @Override
  public void fixedSizeMode(final int i) {
    delegate.fixedSizeMode(i);
  }

  @Override
  @Fluent
  public RecordParser maxRecordSize(final int i) {
    return delegate.maxRecordSize(i);
  }

  @Override
  public void handle(final Buffer buffer) {
    delegate.handle(buffer);
  }

  @Override
  public RecordParser exceptionHandler(final Handler<Throwable> handler) {
    return delegate.exceptionHandler(handler);
  }

  @Override
  public RecordParser handler(final Handler<Buffer> handler) {
    return delegate.handler(handler);
  }

  @Override
  public RecordParser pause() {
    return delegate.pause();
  }

  @Override
  public RecordParser fetch(final long l) {
    return delegate.fetch(l);
  }

  @Override
  public RecordParser resume() {
    synchronized (source) {
      return delegate.resume();
    }
  }

  @Override
  public RecordParser endHandler(final Handler<Void> handler) {
    return delegate.endHandler(handler);
  }
}
