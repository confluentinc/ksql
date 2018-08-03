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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class StreamsList extends KsqlEntity {

  private final Collection<SourceInfo.Stream> streams;

  @JsonCreator
  public StreamsList(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("streams") final Collection<SourceInfo.Stream> streams
  ) {
    super(statementText);
    this.streams = streams;
  }

  public List<SourceInfo.Stream> getStreams() {
    return new ArrayList<>(streams);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StreamsList)) {
      return false;
    }
    final StreamsList that = (StreamsList) o;
    return Objects.equals(getStreams(), that.getStreams());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getStreams());
  }
}
