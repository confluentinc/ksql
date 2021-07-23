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
public final class OutputRecordsNode {

  private final List<RecordNode> outputRecords;

  public OutputRecordsNode(
      @JsonProperty("outputs") final List<RecordNode> outputRecords
  ) throws Exception {
    if (outputRecords == null) {
      throw new Exception("No 'outputs' field in the output file.");
    }
    if (outputRecords.isEmpty()) {
      throw new Exception("Outputs cannot be empty.");
    }
    this.outputRecords = ImmutableList.copyOf(outputRecords);
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "outputRecords is ImmutableList")
  public List<RecordNode> getOutputRecords() {
    return outputRecords;
  }
}
