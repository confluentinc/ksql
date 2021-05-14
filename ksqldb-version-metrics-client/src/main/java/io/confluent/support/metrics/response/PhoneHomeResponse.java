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

package io.confluent.support.metrics.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class PhoneHomeResponse {

  private final String confluentPlatformVersion;
  private final String informationForUser;

  @JsonCreator
  public PhoneHomeResponse(
      @JsonProperty("confluentPlatformVersion") final String confluentPlatformVersion,
      @JsonProperty("informationForUser") final String informationForUser
  ) {
    this.confluentPlatformVersion = confluentPlatformVersion;
    this.informationForUser = informationForUser;
  }

  /**
   * The latest available version of the Confluent Platform or null.
   */
  public String getConfluentPlatformVersion() {
    return confluentPlatformVersion;
  }

  /**
   * Short text with information to the user or null.
   */
  public String getInformationForUser() {
    return informationForUser;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof io.confluent.support.metrics.response.PhoneHomeResponse)) {
      return false;
    }
    final io.confluent.support.metrics.response.PhoneHomeResponse that =
        (io.confluent.support.metrics.response.PhoneHomeResponse) o;
    return Objects.equals(getConfluentPlatformVersion(), that.getConfluentPlatformVersion())
           && Objects.equals(getInformationForUser(), that.getInformationForUser());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getConfluentPlatformVersion(), getInformationForUser());
  }
}
