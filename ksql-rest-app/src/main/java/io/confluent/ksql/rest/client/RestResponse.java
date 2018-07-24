/*
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

package io.confluent.ksql.rest.client;

import java.util.Objects;

import io.confluent.ksql.rest.entity.KsqlErrorMessage;

// Don't tell anybody, but this is basically Haskell's Either datatype...
// I swear it seemed like the best way to do things
public abstract class RestResponse<R> {
  private RestResponse() { }

  public abstract boolean isSuccessful();

  public abstract boolean isErroneous();

  public abstract KsqlErrorMessage getErrorMessage();

  public abstract R getResponse();

  public static <R> RestResponse<R> erroneous(final KsqlErrorMessage errorMessage) {
    return new Erroneous<>(errorMessage);
  }

  public static <R> RestResponse<R> erroneous(final int errorCode, final String message) {
    return new Erroneous<>(
        new KsqlErrorMessage(errorCode, message));
  }

  public static <R> RestResponse<R> successful(final R response) {
    return new Successful<>(response);
  }

  public static <R> RestResponse<R> of(final KsqlErrorMessage errorMessage) {
    return erroneous(errorMessage);
  }

  public static <R> RestResponse<R> of(final R response) {
    return successful(response);
  }

  public Object get() {
    if (isSuccessful()) {
      return getResponse();
    } else {
      return getErrorMessage();
    }
  }

  private static final class Erroneous<R> extends RestResponse<R> {
    private final KsqlErrorMessage errorMessage;

    private Erroneous(final KsqlErrorMessage errorMessage) {
      this.errorMessage = errorMessage;
    }

    @Override
    public boolean isSuccessful() {
      return false;
    }

    @Override
    public boolean isErroneous() {
      return true;
    }

    @Override
    public KsqlErrorMessage getErrorMessage() {
      return errorMessage;
    }

    @Override
    public R getResponse() {
      throw new UnsupportedOperationException();
    }
  }

  private static final class Successful<R> extends RestResponse<R> {
    private final R response;

    private Successful(final R response) {
      this.response = response;
    }

    @Override
    public boolean isSuccessful() {
      return true;
    }

    @Override
    public boolean isErroneous() {
      return false;
    }

    @Override
    public KsqlErrorMessage getErrorMessage() {
      throw new UnsupportedOperationException();
    }

    @Override
    public R getResponse() {
      return response;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Successful)) {
        return false;
      }
      final Successful<?> that = (Successful<?>) o;
      return Objects.equals(getResponse(), that.getResponse());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getResponse());
    }
  }
}
