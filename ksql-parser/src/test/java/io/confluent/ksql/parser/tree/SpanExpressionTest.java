/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.parser.tree;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertEquals;

public class SpanExpressionTest {

  @Test
  public void shouldDisplayCorrectStringWithSingleSpan() {
    SpanExpression expression = new SpanExpression(20, TimeUnit.SECONDS);
    assertEquals(" SPAN 20 SECONDS", expression.toString());
  }

  @Test
  public void shouldDisplayCorrectSTringWithBeforeAndAfter() {
    SpanExpression expression = new SpanExpression(30, 40, TimeUnit.MINUTES, TimeUnit.HOURS);
    assertEquals(" SPAN (30 MINUTES, 40 HOURS)", expression.toString());
  }

}
