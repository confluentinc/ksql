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

package io.confluent.ksql.rest.client;

import com.google.errorprone.annotations.Immutable;
import java.util.Objects;

@Immutable
public final class BasicCredentials {

  private final String username;
  private final String password;

  public static BasicCredentials of(final String username, final String password) {
    return new BasicCredentials(username, password);
  }

  private BasicCredentials(final String username, final String password) {
    this.username = Objects.requireNonNull(username, "username");
    this.password = Objects.requireNonNull(password, "password");
  }

  public String username() {
    return username;
  }

  public String password() {
    return password;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final BasicCredentials that = (BasicCredentials) o;
    return Objects.equals(username, that.username)
        && Objects.equals(password, that.password);
  }

  @Override
  public int hashCode() {
    return Objects.hash(username, password);
  }
}
