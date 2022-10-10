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

package io.confluent.ksql.tools.test;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.tools.test.model.Test;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * Loader of tests.
 *
 * @param <T> the type of test.
 */
public interface TestLoader<T extends Test> {

  // Pass a single test or multiple tests separated by commas to the test framework.
  // Example:
  //   mvn test -pl ksql-engine -Dtest=QueryTranslationTest -Dksql.test.files=test1.json
  //   mvn test -pl ksql-engine -Dtest=QueryTranslationTest -Dksql.test.files=test1.json,test2,json
  String KSQL_TEST_FILES = "ksql.test.files";

  Stream<T> load() throws IOException;

  static List<String> getWhiteList() {
    final String ksqlTestFiles = System.getProperty(KSQL_TEST_FILES, "").trim();
    if (ksqlTestFiles.isEmpty()) {
      return Collections.emptyList();
    }

    return ImmutableList.copyOf(ksqlTestFiles.split(","));
  }
}
