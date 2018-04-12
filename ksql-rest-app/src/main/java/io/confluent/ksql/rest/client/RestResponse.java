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

package io.confluent.ksql.rest.client;

import io.confluent.ksql.rest.entity.KsqlErrorMessage;

import java.util.Objects;

// Don't tell anybody, but this is basically Haskell's Either datatype...
// I swear it seemed like the best way to do things
public abstract class RestResponse<R> {
  private RestResponse() { }

  public abstract boolean isSuccessful();

  public abstract boolean isErroneous();

  public abstract KsqlErrorMessage getErrorMessage();

  public abstract R getResponse();

  public static <R> RestResponse<R> erroneous(KsqlErrorMessage errorMessage) {
    return new Erroneous<>(errorMessage);
  }

  public static <R> RestResponse<R> erroneous(int errorCode, String message) {
    return new Erroneous<>(
        new KsqlErrorMessage(errorCode, message));
  }

  public static <R> RestResponse<R> successful(R response) {
    return new Successful<>(response);
  }

  public static <R> RestResponse<R> of(KsqlErrorMessage errorMessage) {
    return erroneous(errorMessage);
  }

  public static <R> RestResponse<R> of(R response) {
    return successful(response);
  }

  public Object get() {
    if (isSuccessful()) {
      return getResponse();
    } else {
      return getErrorMessage();
    }
  }

  private static class Erroneous<R> extends RestResponse<R> {
    private final KsqlErrorMessage errorMessage;

    public Erroneous(KsqlErrorMessage errorMessage) {
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

  private static class Successful<R> extends RestResponse<R> {
    private final R response;

    public Successful(R response) {
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
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Successful)) {
        return false;
      }
      Successful<?> that = (Successful<?>) o;
      return Objects.equals(getResponse(), that.getResponse());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getResponse());
    }
  }
}
