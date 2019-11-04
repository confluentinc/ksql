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

import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import java.util.Objects;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.HttpStatus.Code;

public abstract class RestResponse<R> {

  private final Code statusCode;

  private RestResponse(final HttpStatus.Code statusCode) {
    this.statusCode = Objects.requireNonNull(statusCode, "statusCode");
  }

  public boolean isSuccessful() {
    return statusCode.isSuccess();
  }

  public boolean isErroneous() {
    return !isSuccessful();
  }

  public abstract KsqlErrorMessage getErrorMessage();

  public abstract R getResponse();

  public HttpStatus.Code getStatusCode() {
    return statusCode;
  }

  public static <R> RestResponse<R> erroneous(
      final HttpStatus.Code statusCode,
      final KsqlErrorMessage errorMessage
  ) {
    return new Erroneous<>(statusCode, errorMessage);
  }

  public static <R> RestResponse<R> erroneous(
      final HttpStatus.Code statusCode,
      final String message
  ) {
    return new Erroneous<>(
        statusCode,
        new KsqlErrorMessage(Errors.toErrorCode(statusCode.getCode()), message)
    );
  }

  public static <R> RestResponse<R> successful(
      final HttpStatus.Code statusCode,
      final R response
  ) {
    return new Successful<>(statusCode, response);
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

    private Erroneous(
        final HttpStatus.Code statusCode,
        final KsqlErrorMessage errorMessage
    ) {
      super(statusCode);
      this.errorMessage = errorMessage;

      if (statusCode.isSuccess()) {
        throw new IllegalArgumentException("Success code passed to error!");
      }
    }

    @Override
    public KsqlErrorMessage getErrorMessage() {
      return errorMessage;
    }

    @Override
    public R getResponse() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
      return "Erroneous{"
          + "statusCode=" + getStatusCode()
          + ", errorMessage=" + errorMessage
          + '}';
    }
  }

  private static final class Successful<R> extends RestResponse<R> {

    private final R response;

    private Successful(
        final HttpStatus.Code statusCode,
        final R response
    ) {
      super(statusCode);
      this.response = response;

      if (!statusCode.isSuccess()) {
        throw new IllegalArgumentException("Error code passed to success!");
      }
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
      return Objects.equals(getResponse(), that.getResponse())
          && Objects.equals(getStatusCode(), that.getStatusCode());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getResponse(), getStatusCode());
    }

    @Override
    public String toString() {
      return "Successful{"
          + "statusCode=" + getStatusCode()
          + ", response=" + response
          + '}';
    }
  }
}
