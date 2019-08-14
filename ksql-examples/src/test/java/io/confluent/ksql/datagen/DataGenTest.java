/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.datagen;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import io.confluent.ksql.util.KsqlConfig;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DataGenTest {
  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test(expected = DataGen.Arguments.ArgumentParseException.class)
  public void shouldThrowOnUnknownFormat() throws Exception {
    DataGen.run(
        "format=wtf",
        "schema=./src/main/resources/purchase.avro",
        "topic=foo",
        "key=id");
  }

  @Test
  public void shouldThrowIfSchemaFileDoesNotExist() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(containsString("File not found: you/won't/find/me/right?"));

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

  @Test
  public void shouldPassSchemaRegistryUrl() throws Exception {
    final DataGen.Arguments args = new DataGen.Arguments(
        false,
        "bootstrap",
        null,
        null,
        null,
        "topic",
        "key",
        0,
        0L,
        "srUrl",
        null
    );

    final Properties props = DataGen.getProperties(args);
    assertThat(props.getProperty(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY), equalTo("srUrl"));
  }
}