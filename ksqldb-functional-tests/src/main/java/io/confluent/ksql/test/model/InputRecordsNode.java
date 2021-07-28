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

package io.confluent.ksql.test.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class InputRecordsNode {

  private final List<RecordNode> inputRecords;

  public InputRecordsNode(
      @JsonProperty("inputs") final List<RecordNode> inputRecords
  ) throws Exception {
    if (inputRecords == null) {
      throw new Exception("No 'inputs' field in the input file.");
    }
    if (inputRecords.isEmpty()) {
      throw new Exception("Inputs cannot be empty.");
    }

    this.inputRecords = ImmutableList.copyOf(inputRecords);
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "inputRecords is ImmutableList")
  public List<RecordNode> getInputRecords() {
    return inputRecords;
  }
}
