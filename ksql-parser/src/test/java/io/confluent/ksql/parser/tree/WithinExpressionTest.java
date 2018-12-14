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

package io.confluent.ksql.parser.tree;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class WithinExpressionTest {

  @Test
  public void shouldDisplayCorrectStringWithSingleWithin() {
    final WithinExpression expression = new WithinExpression(20, TimeUnit.SECONDS);
    assertEquals(" WITHIN 20 SECONDS", expression.toString());
  }

  @Test
  public void shouldDisplayCorrectStringWithBeforeAndAfter() {
    final WithinExpression expression = new WithinExpression(30, 40, TimeUnit.MINUTES, TimeUnit.HOURS);
    assertEquals(" WITHIN (30 MINUTES, 40 HOURS)", expression.toString());
  }

}
