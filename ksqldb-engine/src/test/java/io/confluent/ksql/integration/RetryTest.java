/*
 * Copyright 2021 Confluent Inc.
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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;

import io.confluent.ksql.util.KsqlException;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RetryTest {

  // This resource is an ExternalResource rule, which will be run as part
  // of RESOURCE_RETRY_CHAIN. The first time that it is run, it throws
  // a RetryException and increments some global state. The second time it
  // is run, it succeeds. (see code at the bottom of the file)
  private static final RetryResource RESOURCE = new RetryResource(1);

  // Since we are retrying RESOURCE once, this should not cause the test to
  // fail if the Retry rule does its job correctly. This is, in essence, a
  // test in and of itself and is not related to any of the unit tests (it
  // is run once as the class is setup, unlike @Before which is run before
  // every unit test)
  @ClassRule
  public static final RuleChain RESOURCE_RETRY_CHAIN = RuleChain
      .outerRule(Retry.of(1, RetryException.class, 0, TimeUnit.SECONDS))
      .around(RESOURCE);

  @Rule
  public Retry retry = Retry.none();

  @Mock
  public TimeUnit timeUnit;

  // initialize outside of @Before in case @Before is run multiple times
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
  public void shouldSucceedOnFirstRetryWithWait() throws InterruptedException {
    // Given:
    doNothing().when(timeUnit).sleep(anyLong());
    retry.withDelay(10, timeUnit);
    test++;

    // When:
    if (test == 1) throw new RetryException(test);

    // Then:
    verify(timeUnit).sleep(10);
    assertThat(test, equalTo(2));
  }

  @Test
  public void shouldFailOnSecondRetry() {
    // Given:
    test++;

    // When:
    if (test < 2) {
      throw new RetryException(test);
    }

    // Then:
    assertThat(test, equalTo(2));
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

    // When:
    if (test < 1) {
      throw new RetryException(test);
    }

    // Then:
    assertThat(test, equalTo(1));
  }

  @Test
  public void shouldNotRetryTestsWithDifferingExceptions() {
    // Given:
    test++;
    retry.when(KsqlException.class);

    // When:
    if (test < 1) {
      throw new RetryException(test);
    }

    // Then:
    assertThat(test, equalTo(1));
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
