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

package io.confluent.ksql.test.commons;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class QttTestFile {

  public final List<TestCaseNode> tests;

  QttTestFile(@JsonProperty("tests") final List<TestCaseNode> tests) {
    this.tests = ImmutableList.copyOf(requireNonNull(tests, "tests collection missing"));

    if (tests.isEmpty()) {
      throw new IllegalArgumentException("test file did not contain any tests");
    }
  }
}