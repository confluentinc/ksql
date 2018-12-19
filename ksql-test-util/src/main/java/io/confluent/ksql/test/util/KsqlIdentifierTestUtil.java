/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.test.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test utility class for ensuring unique identifier names in KSQL statements.
 *
 * <p>e.g. Ksql stream and table names.
 */
public final class KsqlIdentifierTestUtil {

  private static final AtomicInteger COUNTER = new AtomicInteger();

  private KsqlIdentifierTestUtil() {
  }

  /**
   * Use this method to create a unique name for streams or topics in KSQL statements.
   *
   * <p>This stops one tests from interfering with each other - a common gotcha!
   *
   * @param prefix a prefix for the name, (to help more easily identify it).
   * @return a unique name that matches KSQL's allowed format for identifiers, starting with {@code
   * prefix}.
   */
  public static String uniqueIdentifierName(final String prefix) {
    return prefix + '_' + uniqueIdentifierName();
  }

  /**
   * Use this method to create a unique name for streams or topics in KSQL statements.
   *
   * <p>This stops one tests from interfering with each other - a common gotcha!
   *
   * @return a unique name that matches KSQL's allowed format for identifiers.
   */
  public static String uniqueIdentifierName() {
    return "ID_" + COUNTER.getAndIncrement();
  }
}
