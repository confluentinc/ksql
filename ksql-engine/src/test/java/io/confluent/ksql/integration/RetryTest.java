/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.integration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import io.confluent.ksql.util.KsqlException;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;

public class RetryTest {

  // this resource should not cause the test to fail because we have
  // a chained Retry outer rule
  private static final RetryResource RESOURCE = new RetryResource(1);

  @ClassRule public static final RuleChain RESOURCE_RETRY_CHAIN = RuleChain
      .outerRule(Retry.of(1, RetryException.class, 0, TimeUnit.SECONDS))
      .around(RESOURCE);

  @Rule public Retry retry = Retry.none();
  @Rule public ExpectedException expectedException = ExpectedException.none();

  // initialize before @Before in case @Before is run multiple times
  private int test = 0;

  @Before
  public void setUpWhichFailsOnce() throws RetryException {
    retry.upTo(1);
    retry.when(RetryException.class);
  }

  @Test
  public void shouldSucceedOnFirstRetry() {
    // Given:
    test++;

    // When:
    if (test == 1) throw new RetryException(test);

    // Then:
    assertThat(test, equalTo(2));
  }

  @Test
  public void shouldFailOnSecondRetry() {
    // Given:
    test++;

    // Expect:
    if (test == 2) {
      expectedException.expect(RetryException.class);
      expectedException.expectMessage("2");
    }

    // When:
    throw new RetryException(test);
  }

  @Test
  public void shouldOverrideRetryInBefore() {
    // Given:
    test++;
    retry.upTo(2);

    // When:
    if (test < 3) throw new RetryException(test);

    // Then:
    assertThat(test, equalTo(3));
  }

  @Test
  public void shouldOverrideRetryInBeforeAndFail() {
    // Given:
    test++;
    retry.upTo(0);

    // Expect:
    expectedException.expect(RetryException.class);

    // When:
    if (test == 1) throw new RetryException(test);
  }

  @Test
  public void shouldNotRetryTestsWithDifferingExceptions() {
    // Given:
    test++;
    retry.when(KsqlException.class);

    // Expect:
    expectedException.expect(RetryException.class);
    expectedException.expectMessage("1");

    // When:
    throw new RetryException(test);
  }

  private static class RetryResource extends ExternalResource {
    final int minAttempts;
    int attempts = 0;

    private RetryResource(final int minAttempts) {
      this.minAttempts = minAttempts;
    }

    @Override
    protected void before() {
      attempts++;
      if (minAttempts >= attempts) {
        throw new RetryException(attempts);
      }
    }
  }

  private static class RetryException extends RuntimeException {
    RetryException(final int attemptNumber) {
      super(String.valueOf(attemptNumber));
    }
  }
}
