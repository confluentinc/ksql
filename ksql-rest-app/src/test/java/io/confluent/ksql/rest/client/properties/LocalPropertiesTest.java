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

package io.confluent.ksql.rest.client.properties;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.config.PropertyParser;
import java.util.Map;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class LocalPropertiesTest {

  private static final Map<String, Object> INITIAL = ImmutableMap.of(
      "prop-1", "initial-val-1",
      "prop-2", "initial-val-2"
  );

  @Mock(MockType.NICE)
  private PropertyParser parser;
  private LocalProperties props;

  @Before
  public void setUp() {
    props = new LocalProperties(INITIAL, parser);

    expect(parser.parse(anyString(), anyObject()))
        .andReturn("parsed-val");

    replay(parser);
  }

  @Test
  public void shouldGetInitialValues() {
    assertThat(props.toMap().get("prop-1"), is("initial-val-1"));
    assertThat(props.toMap().get("prop-2"), is("initial-val-2"));
  }

  @Test
  public void shouldUnsetInitialValue() {
    // When:
    final Object oldValue = props.unset("prop-1");

    // Then:
    assertThat(oldValue, is("initial-val-1"));
    assertThat(props.toMap().get("prop-1"), is(nullValue()));
    assertThat(props.toMap().get("prop-2"), is("initial-val-2"));
  }

  @Test
  public void shouldOverrideInitialValue() {
    // When:
    final Object oldValue = props.set("prop-1", "new-val");

    // Then:
    assertThat(oldValue, is("initial-val-1"));
    assertThat(props.toMap().get("prop-1"), is("parsed-val"));
    assertThat(props.toMap().get("prop-2"), is("initial-val-2"));
  }

  @Test
  public void shouldUnsetOverriddenValue() {
    // Given:
    props.set("prop-1", "new-val");

    // When:
    final Object oldValue = props.unset("prop-1");

    // Then:
    assertThat(oldValue, is("parsed-val"));
    assertThat(props.toMap().get("prop-1"), is(nullValue()));
    assertThat(props.toMap().get("prop-2"), is("initial-val-2"));
  }

  @Test
  public void shouldReturnOnlyKnownKeys() {
    assertThat(props.toMap().keySet(), containsInAnyOrder("prop-1", "prop-2"));
  }

  @Test
  public void shouldSetNewValue() {
    // When:
    final Object oldValue = props.set("new-prop", "new-val");

    // Then:
    assertThat(oldValue, is(nullValue()));
    assertThat(props.toMap().get("new-prop"), is("parsed-val"));
    assertThat(props.toMap().get("prop-2"), is("initial-val-2"));
  }

  @Test
  public void shouldUnsetNewValue() {
    // Given:
    props.set("new-prop", "new-val");

    // When:
    final Object oldValue = props.unset("new-prop");

    // Then:
    assertThat(oldValue, is("parsed-val"));
    assertThat(props.toMap().get("new-prop"), is(nullValue()));
    assertThat(props.toMap().get("prop-2"), is("initial-val-2"));
  }

  @Test
  public void shouldInvokeParserCorrectly() {
    // Given:
    reset(parser);
    expect(parser.parse("prop-1", "new-val")).andReturn("parsed-new-val");
    replay(parser);

    // When:
    props.set("prop-1", "new-val");

    // Then:
    assertThat(props.toMap().get("prop-1"), is("parsed-new-val"));
    verify(parser);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfParserThrows() {
    // Given:
    reset(parser);
    expect(parser.parse("prop-1", "new-val"))
        .andThrow(new IllegalArgumentException("Boom"));
    replay(parser);

    // When:
    props.set("prop-1", "new-val");
  }
}