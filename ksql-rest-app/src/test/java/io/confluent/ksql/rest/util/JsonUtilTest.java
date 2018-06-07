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

package io.confluent.ksql.rest.util;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.node.IntNode;

import org.junit.Assert;
import org.junit.Test;

import io.confluent.ksql.GenericRow;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

public class JsonUtilTest {

  @Test
  public void shouldCreateCorrectRow() throws IOException {
    GenericRow genericRow = JsonUtil.buildGenericRowFromJson("{\"f1\":0}");
    assertThat(genericRow.getColumns().size(), equalTo(1));
    assertThat(genericRow.getColumns().get(0), instanceOf(IntNode.class));
    IntNode intNode = (IntNode) genericRow.getColumns().get(0);
    assertThat(intNode.intValue(), equalTo(0));
  }

  @Test
  public void shouldFailForIncorrectRow() {
    try {
      GenericRow genericRow = JsonUtil.buildGenericRowFromJson("{\"f1\"0}");
      Assert.fail();
    } catch (IOException e) {
      assertThat(e, instanceOf(JsonParseException.class));
    }
  }

}