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

package io.confluent.ksql.test.planned;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.test.model.SchemaNode;
import io.confluent.ksql.test.model.TestCaseNode;
import java.util.Map;

@JsonInclude(Include.NON_ABSENT)
public class TestCaseSpecNode {

  private final String version;
  private final long timestamp;
  private final String path;
  private final Map<String, SchemaNode> schemas;
  private final TestCaseNode testCase;

  public TestCaseSpecNode(
      @JsonProperty("version") final String version,
      @JsonProperty("timestamp") final long timestamp,
      @JsonProperty("path") final String path,
      @JsonProperty("schemas") final Map<String, SchemaNode> schemas,
      @JsonProperty("testCase") final TestCaseNode testCase
  ) {
    this.version = requireNonNull(version, "version");
    this.timestamp = timestamp;
    this.path = requireNonNull(path, "path");
    this.schemas = requireNonNull(schemas, "schemas");
    this.testCase = requireNonNull(testCase, "testCase");
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getPath() {
    return path;
  }

  public Map<String, SchemaNode> getSchemas() {
    return schemas;
  }

  public String getVersion() {
    return version;
  }

  public TestCaseNode getTestCase() {
    return testCase;
  }
}
