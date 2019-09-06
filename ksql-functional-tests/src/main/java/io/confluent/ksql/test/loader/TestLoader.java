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

package io.confluent.ksql.test.loader;

import io.confluent.ksql.test.tools.Test;
import java.util.stream.Stream;

/**
 * Loader of tests.
 *
 * @param <T> the type of test.
 */
public interface TestLoader<T extends Test> {

  Stream<T> load();
}
