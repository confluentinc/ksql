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

package io.confluent.ksql.rest.server.execution;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.rest.entity.PropertiesList.Property;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.test.util.KsqlTestFolder;
import io.confluent.ksql.util.KsqlConfig;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ListPropertiesExecutorTest {

  @Rule
  public final TemporaryEngine engine = new TemporaryEngine();

  @Rule
  public TemporaryFolder folder = KsqlTestFolder.temporaryFolder();

  private String connectPropsFile;

  @Before
  public void setUp() {
    connectPropsFile = Paths.get(folder.getRoot().getPath(), "connect.properties").toString();
  }

  @Test
  public void shouldContainAllowField() {
    // When:
    final PropertiesList properties = (PropertiesList) CustomExecutors.LIST_PROPERTIES.execute(
        engine.configure("LIST PROPERTIES;"),
        mock(SessionProperties.class),
        engine.getEngine(),
        engine.getServiceContext()
    ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(toMap(properties).get("ksql.streams.commit.interval.ms").getEditable(), equalTo(true));
    assertThat(toMap(properties).get(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG).getEditable(), equalTo(false));
  }

  @Test
  public void shouldContainLevelField() {
    // When:
    final PropertiesList properties = (PropertiesList) CustomExecutors.LIST_PROPERTIES.execute(
        engine.configure("LIST PROPERTIES;"),
        mock(SessionProperties.class),
        engine.getEngine(),
        engine.getServiceContext()
    ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(toMap(properties).get(KsqlConfig.KSQL_EXT_DIR).getLevel(), equalTo("SERVER"));
    assertThat(toMap(properties).get(KsqlConfig.KSQL_STRING_CASE_CONFIG_TOGGLE).getLevel(), equalTo("QUERY"));
  }

  @Test
  public void shouldListProperties() {
    // When:
    final PropertiesList properties = (PropertiesList) CustomExecutors.LIST_PROPERTIES.execute(
        engine.configure("LIST PROPERTIES;"),
        mock(SessionProperties.class),
        engine.getEngine(),
        engine.getServiceContext()
    ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(
        toStringMap(properties),
        equalTo(engine.getKsqlConfig().getAllConfigPropsWithSecretsObfuscated()));
    assertThat(properties.getOverwrittenProperties(), is(empty()));
  }

  @Test
  public void shouldListPropertiesWithOverrides() {
    // When:
    final PropertiesList properties = (PropertiesList) CustomExecutors.LIST_PROPERTIES.execute(
        engine.configure("LIST PROPERTIES;")
            .withConfigOverrides(ImmutableMap.of("ksql.streams.auto.offset.reset", "latest")),
        mock(SessionProperties.class),
        engine.getEngine(),
        engine.getServiceContext()
    ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(
        properties.getProperties(),
        hasItem(new Property("ksql.streams.auto.offset.reset", "KSQL", "latest")));
    assertThat(properties.getOverwrittenProperties(), hasItem("ksql.streams.auto.offset.reset"));
  }

  @Test
  public void shouldNotListSslProperties() {
    // When:
    final PropertiesList properties = (PropertiesList) CustomExecutors.LIST_PROPERTIES.execute(
        engine.configure("LIST PROPERTIES;"),
        mock(SessionProperties.class),
        engine.getEngine(),
        engine.getServiceContext()
    ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(
        toMap(properties),
        not(hasKey(isIn(KsqlConfig.SSL_CONFIG_NAMES))));
  }

  @Test
  public void shouldListUnresolvedStreamsTopicProperties() {
    // When:
    final PropertiesList properties = (PropertiesList) CustomExecutors.LIST_PROPERTIES.execute(
        engine.configure("LIST PROPERTIES;")
            .withConfig(new KsqlConfig(ImmutableMap.of(
                "ksql.streams.topic.min.insync.replicas", "2"))),
        mock(SessionProperties.class),
        engine.getEngine(),
        engine.getServiceContext()
    ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(
        properties.getProperties(),
        hasItem(new Property("ksql.streams.topic.min.insync.replicas", "KSQL", "2")));
  }

  @Test
  public void shouldNotListUnrecognizedConnectProps() throws Exception {
    // Given:
    givenConnectWorkerProperties(
        "group.id=list_properties_unit_test\n"
            + "key.converter=io.confluent.connect.avro.AvroConverter\n"
            + "value.converter=io.confluent.connect.avro.AvroConverter\n"
            + "offset.storage.topic=topic1\n"
            + "config.storage.topic=topic2\n"
            + "status.storage.topic=topic3\n"
            + "other.config=<potentially sensitive data that should not be shown>\n"
            + "sasl.jaas.config=<potentially sensitive data that should not be shown even though it's a recognized config>\n"
    );

    // When:
    final PropertiesList properties = (PropertiesList) CustomExecutors.LIST_PROPERTIES.execute(
        engine.configure("LIST PROPERTIES;")
            .withConfig(new KsqlConfig(ImmutableMap.of(
                "ksql.connect.worker.config", connectPropsFile))),
        mock(SessionProperties.class),
        engine.getEngine(),
        engine.getServiceContext()
    ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(
        properties.getProperties(),
        hasItem(new Property("ksql.connect.worker.config", "KSQL", connectPropsFile)));
    assertThat(
        properties.getProperties(),
        hasItem(new Property("value.converter", "EMBEDDED CONNECT WORKER", "io.confluent.connect.avro.AvroConverter")));
    assertThat(toMap(properties), not(hasKey("other.config")));
    assertThat(toMap(properties), not(hasKey("sasl.jaas.config")));
  }

  private void givenConnectWorkerProperties(final String contents) throws Exception {
    assertThat(new File(connectPropsFile).createNewFile(), is(true));

    try (PrintWriter out = new PrintWriter(connectPropsFile, Charset.defaultCharset().name())) {
      out.println(contents);
    } catch (FileNotFoundException | UnsupportedEncodingException e) {
      Assert.fail("Failed to write connect worker config file: " + connectPropsFile);
    }
  }

  private static Map<String, Property> toMap(final PropertiesList properties) {
    final Map<String, Property> map = new HashMap<>();
    for (final Property property : properties.getProperties()) {
      map.put(property.getName(), property);
    }
    return map;
  }

  private static Map<String, String> toStringMap(final PropertiesList properties) {
    final Map<String, String> map = new HashMap<>();
    for (final Property property : properties.getProperties()) {
      map.put(property.getName(), property.getValue());
    }
    return map;
  }
}
