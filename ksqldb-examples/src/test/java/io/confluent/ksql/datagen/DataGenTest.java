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
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MockSystemExit;
import java.util.Properties;
import org.junit.Test;

public class DataGenTest {
  private final static MockSystemExit mockSystem = new MockSystemExit();

  @Test(expected = DataGen.Arguments.ArgumentParseException.class)
  public void shouldThrowOnUnknownFormat() throws Throwable {
    DataGen.run(
        mockSystem,
        "format=wtf",
        "schema=./src/main/resources/purchase.avro",
        "topic=foo",
        "key=id"
    );
  }

  @Test
  public void shouldThrowIfSchemaFileDoesNotExist() throws Throwable {
    // When:
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> DataGen.run(
            mockSystem,
            "schema=you/won't/find/me/right?",
            "format=avro",
            "topic=foo",
            "key=id"
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "File not found: you/won't/find/me/right?"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfKeyFieldDoesNotExist() throws Throwable {
    DataGen.run(
        mockSystem,
        "key=not_a_field",
        "schema=./src/main/resources/purchase.avro",
        "format=avro",
        "topic=foo"
    );
  }

  @Test(expected = DataGen.Arguments.ArgumentParseException.class)
  public void shouldThrowOnUnknownQuickStart() throws Throwable {
    DataGen.run(
        mockSystem,
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
        null,
        "topic",
        "key",
        null,
        0,
        "srUrl",
        null,
        1,
        -1,
        true
    );

    final Properties props = DataGen.getProperties(args);
    assertThat(props.getProperty(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY), equalTo("srUrl"));
  }

  @Test(expected = DataGen.Arguments.ArgumentParseException.class)
  public void valueDelimiterCanOnlyBeSingleCharacter() throws Throwable {
    DataGen.run(
        mockSystem,
        "schema=./src/main/resources/purchase.avro",
        "key=id",
        "format=delimited",
        "value_delimiter=@@",
        "topic=foo"
    );
  }
}