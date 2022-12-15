/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.statement;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.TableElements;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SourcePropertyInjectorTest {

  @Mock
  private SessionConfig sessionConfig;

  @Mock
  private CreateSource createSource;
  @Mock
  private ConfiguredStatement<CreateSource> csStatement;
  @Mock
  private CreateSourceProperties originalCSProps;
  @Mock
  private CreateSourceProperties csPropsWithUnwrapping;
  @Mock
  private TableElements tableElements;
  @Mock
  private CreateSource csWithUnwrapping;

  @Mock
  private CreateAsSelect createAsSelect;
  @Mock
  private ConfiguredStatement<CreateAsSelect> csasStatement;
  @Mock
  private CreateSourceAsProperties originalCsasProps;
  @Mock
  private CreateSourceAsProperties csasPropsWithUnwrapping;
  @Mock
  private CreateAsSelect csasWithUnwrapping;

  private SourcePropertyInjector injector;

  @Before
  public void setUp() {
    when(csStatement.getStatement()).thenReturn(createSource);
    when(csStatement.getSessionConfig()).thenReturn(sessionConfig);
    when(createSource.getProperties()).thenReturn(originalCSProps);
    when(createSource.getElements()).thenReturn(tableElements);
    when(originalCSProps.withUnwrapProtobufPrimitives(true)).thenReturn(csPropsWithUnwrapping);
    when(createSource.copyWith(tableElements, csPropsWithUnwrapping)).thenReturn(csWithUnwrapping);

    when(csasStatement.getStatement()).thenReturn(createAsSelect);
    when(csasStatement.getSessionConfig()).thenReturn(sessionConfig);
    when(createAsSelect.getProperties()).thenReturn(originalCsasProps);
    when(originalCsasProps.withUnwrapProtobufPrimitives(true)).thenReturn(csasPropsWithUnwrapping);
    when(createAsSelect.copyWith(csasPropsWithUnwrapping)).thenReturn(csasWithUnwrapping);

    injector = new SourcePropertyInjector();
  }

  @Test
  public void shouldInjectForCreateSource() {
    // When:
    final ConfiguredStatement<CreateSource> configured = injector.inject(csStatement);

    // Then:
    assertThat(configured.getStatement(), is(csWithUnwrapping));
  }

  @Test
  public void shouldInjectForCreateAsSelect() {
    // When:
    final ConfiguredStatement<CreateAsSelect> configured = injector.inject(csasStatement);

    // Then:
    assertThat(configured.getStatement(), is(csasWithUnwrapping));
  }

}