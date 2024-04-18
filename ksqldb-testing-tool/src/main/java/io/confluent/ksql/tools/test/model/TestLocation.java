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

package io.confluent.ksql.tools.test.model;

import java.nio.file.Path;

/**
 * Location of test.
 *
 * <p>{@link #toString()} should return a file path e.g. {@code file://some/path/to/file}
 */
public interface TestLocation {

  /**
   * @return the path to the file containing the test.
   */
  Path getTestPath();
}
