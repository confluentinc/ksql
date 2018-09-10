/*
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
 */

package io.confluent.ksql.datagen;

import java.io.IOException;
import org.junit.Test;

public class DataGenTest {

  @Test(expected = DataGen.Arguments.ArgumentParseException.class)
  public void shouldThrowOnUnknownFormat() throws Exception {
    DataGen.run(
        "format=wtf",
        "schema=./src/main/resources/purchase.avro",
        "topic=foo",
        "key=id");
  }

  @Test(expected = IOException.class)
  public void shouldThrowIfSchemaFileDoesNotExist() throws Exception {
    DataGen.run(
        "schema=you/won't/find/me/right?",
        "format=avro",
        "topic=foo",
        "key=id");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfKeyFieldDoesNotExist() throws Exception {
    DataGen.run(
        "key=not_a_field",
        "schema=./src/main/resources/purchase.avro",
        "format=avro",
        "topic=foo");
  }

  @Test(expected = DataGen.Arguments.ArgumentParseException.class)
  public void shouldThrowOnUnknownQuickStart() throws Exception {
    DataGen.run(
        "quickstart=wtf",
        "format=avro",
        "topic=foo");
  }
}