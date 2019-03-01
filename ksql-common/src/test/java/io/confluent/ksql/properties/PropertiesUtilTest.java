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

package io.confluent.ksql.properties;

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlException;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class PropertiesUtilTest {

  @ClassRule
  public static final TemporaryFolder TMP = new TemporaryFolder();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private File propsFile;

  @Before
  public void setUp() throws Exception {
    propsFile = TMP.newFile();
  }

  @Test
  public void shouldLoadPropsFromFile() {
    // Given:
    givenPropsFileContains(
        "# Comment" + System.lineSeparator()
            + "some.prop=some value" + System.lineSeparator()
            + "some.other.prop=124" + System.lineSeparator()
    );

    // When:
    final Map<String, String> result = PropertiesUtil.loadProperties(propsFile);

    // Then:
    assertThat(result.get("some.prop"), is("some value"));
    assertThat(result.get("some.other.prop"), is("124"));
  }

  @Test
  public void shouldLoadImmutablePropsFromFile() {
    // Given:
    givenPropsFileContains(
        "some.prop=some value" + System.lineSeparator()
    );

    final Map<String, String> result = PropertiesUtil.loadProperties(propsFile);

    // Then:
    expectedException.expect(UnsupportedOperationException.class);

    // When:
    result.put("new", "value");
  }

  @Test
  public void shouldThrowIfFailedToLoadProps() {
    // Given:
    propsFile = new File("i_do_not_exist");

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Failed to load properties file: i_do_not_exist");

    // When:
    PropertiesUtil.loadProperties(propsFile);
  }

  @Test
  public void shouldThrowIfPropsFileContainsBlackListedProps() {
    // Given:
    givenPropsFileContains(
        "java.some.disallowed.setting=something" + System.lineSeparator()
            + "java.not.another.one=v"
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Property file contains the following blacklisted properties");
    expectedException.expectMessage("java.some.disallowed.setting");
    expectedException.expectMessage("java.not.another.one");

    // When:
    PropertiesUtil.loadProperties(propsFile);
  }

  @Test
  public void shouldApplyOverrides() {
    // Given:
    final Map<String, String> initial = ImmutableMap.of(
        "should.be.overridden", "initial value",
        "should.be.not.overridden", "initial value"
    );

    final Properties overrides = properties(
        "should.be.overridden", "new value",
        "additional.override", "value");

    // When:
    final Map<String, ?> result = PropertiesUtil.applyOverrides(initial, overrides);

    // Then:
    assertThat(result.get("should.be.overridden"), is("new value"));
    assertThat(result.get("should.be.not.overridden"), is("initial value"));
    assertThat(result.get("additional.override"), is("value"));
  }

  @Test
  public void shouldFilterBlackListedFromOverrides() {
    Stream.of("java.", "os.", "sun.", "user.", "line.separator", "path.separator", "file.separator")
        .forEach(blackListed -> {
          // Given:
          final Properties overrides = properties(
              blackListed + "props.should.be.filtered", "unexpected",
              "should.not.be.filtered", "value"
          );

          // When:
          final Map<String, ?> result = PropertiesUtil.applyOverrides(emptyMap(), overrides);

          // Then:
          assertThat(result.keySet(), hasItem("should.not.be.filtered"));
          assertThat(result.keySet(), not(hasItem("props.should.be.filtered")));
        });
  }

  @Test
  public void shouldFilterByKey() {
    // Given:
    final Map<String, String> props = ImmutableMap.of(
        "keep.this", "v0",
        "keep that", "v1",
        "do not keep this", "keep"
    );

    // When:
    final Map<String, String> result = PropertiesUtil
        .filterByKey(props, key -> key.startsWith("keep"));

    // Then:
    assertThat(result.keySet(), containsInAnyOrder("keep.this", "keep that"));
    assertThat(result.get("keep.this"), is("v0"));
    assertThat(result.get("keep that"), is("v1"));
  }

  private void givenPropsFileContains(final String contents) {
    try {
      Files.write(propsFile.toPath(), contents.getBytes(StandardCharsets.UTF_8));
    } catch (final Exception e) {
      throw new AssertionError("Invalid test: failed to set props file content", e);
    }
  }

  private static Properties properties(final String... s) {
    assertThat(s.length % 2, is(0));

    final Properties props = new Properties();
    for (int i = 0; i < s.length; i = i + 2) {
      props.put(s[i], s[i+1]);
    }
    return props;
  }
}