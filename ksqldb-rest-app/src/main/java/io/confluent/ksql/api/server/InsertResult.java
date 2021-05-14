/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.api.server;

public interface InsertResult {

  long sequenceNumber();

  boolean succeeded();

  Exception exception();

  static InsertResult succeededInsert(final long sequenceNumber) {
    return new InsertResult() {
      @Override
      public long sequenceNumber() {
        return sequenceNumber;
      }

      @Override
      public boolean succeeded() {
        return true;
      }

      @Override
      public Exception exception() {
        return null;
      }
    };
  }

  static InsertResult failedInsert(final long sequenceNumber, final Exception exception) {
    return new InsertResult() {
      @Override
      public long sequenceNumber() {
        return sequenceNumber;
      }

      @Override
      public boolean succeeded() {
        return false;
      }

      @Override
      public Exception exception() {
        return exception;
      }
    };
  }

}
