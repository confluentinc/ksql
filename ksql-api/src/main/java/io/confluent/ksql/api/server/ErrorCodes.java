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

package io.confluent.ksql.api.server;

/**
 * The error codes that signify different types of errors that can occur in the API
 */
public final class ErrorCodes {

  private ErrorCodes() {
  }

  public static final int ERROR_CODE_MISSING_PARAM = 50001;
  public static final int ERROR_CODE_UNKNOWN_QUERY_ID = 50002;
  public static final int ERROR_CODE_INTERNAL_ERROR = 50003;
}
