/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSubTypes({})
public class ClusterTerminateRequest {

  public static final String DELETE_TOPIC_LIST_PROP = "deleteTopicList";

  private final ImmutableList<String> deleteTopicList;

  public ClusterTerminateRequest(
      @JsonProperty(DELETE_TOPIC_LIST_PROP) final List<String> deleteTopicList
  ) {
    this.deleteTopicList = deleteTopicList == null
        ? ImmutableList.of()
        : ImmutableList.copyOf(deleteTopicList);
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "deleteTopicList is ImmutableList")
  public List<String> getDeleteTopicList() {
    return deleteTopicList;
  }

  @JsonIgnore
  public Map<String, Object> getStreamsProperties() {
    return Collections.singletonMap(DELETE_TOPIC_LIST_PROP, deleteTopicList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deleteTopicList);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof ClusterTerminateRequest)) {
      return false;
    }
    final ClusterTerminateRequest that = (ClusterTerminateRequest) o;
    return Objects.equals(deleteTopicList, that.deleteTopicList);
  }

  @Override
  public String toString() {
    return "ClusterTerminateRequest{"
        + "deleteTopicList=" + deleteTopicList
        + '}';
  }
}
