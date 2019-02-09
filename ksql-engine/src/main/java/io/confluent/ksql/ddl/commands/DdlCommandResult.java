/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.ddl.commands;

import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class DdlCommandResult {

  private final boolean success;
  private final String message;

  public DdlCommandResult(final boolean success, final String message) {
    this.success = success;
    this.message = Objects.requireNonNull(message, "message");
  }

  public boolean isSuccess() {
    return success;
  }

  public String getMessage() {
    return message;
  }
}
