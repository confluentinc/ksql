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

package io.confluent.ksql.engine;

import io.confluent.ksql.util.KsqlConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TopicAccessValidatorFactoryTest {
  @Mock
  private KsqlConfig ksqlConfig;

  @Test
  public void shouldReturnAuthorizationValidator() {
    // Given:
    givenTopicAuthorizationConfig(true);

    // When:
    final TopicAccessValidator validator = TopicAccessValidatorFactory.create(ksqlConfig, null);

    // Then
    assertThat(validator, is(instanceOf(AuthorizationTopicAccessValidator.class)));
  }

  @Test
  public void shouldReturnDummyValidator() {
    // Given:
    givenTopicAuthorizationConfig(false);

    // When:
    final TopicAccessValidator validator = TopicAccessValidatorFactory.create(ksqlConfig, null);

    // Then
    assertThat(validator, not(instanceOf(AuthorizationTopicAccessValidator.class)));
  }

  private void givenTopicAuthorizationConfig(final boolean value) {
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_TOPIC_AUTHORIZATION_ENABLED)).thenReturn(value);
  }
}
