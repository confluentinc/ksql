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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code Retry} rule allows you to retry a test that
 * throws a specific exception.
 *
 * <h3>Usage</h3>
 *
 * <pre>
 * public class SimpleRetryTest {
 *  &#064;Rule public Retry retry = Retry.none();
 *
 *  private int attemptCount = 0;
 *
 *  &#064;Test
 *  public void noRetries() {
 *    // this test will fail
 *    throw new RuntimeException();
 *  }
 *
 *  &#064;Test
 *  public void retryOnce() {
 *    retry.when(RuntimeException.class);
 *    retry.upTo(1);
 *
 *    if (attemptCount == 0) {
 *      // test will be retried the first time, and will succeed
 *      // (not throw an exception) the second time
 *      attemptCount++;
 *      throw new RuntimeException();
 *    }
 *  }
 * }
 * </pre>
 *
 * <p>
 * You have to add the {@code Retry} rule to your test; this does not
 * affect your existing tests, as by default no retries will be added.
 * To specify your retry rule, use the three method below:
 *
 * <ul>
 *   <li>{@link #upTo(int)} denotes how many retries to attempt before
 *   considering the test failed. The default is 0, which represents
 *   no retries (attempt only once).</li>
 *   <li>{@link #when(Class)} denotes on which exceptions to retry. The
 *   default is {@link Throwable}.</li>
 *   <li>{@link #withDelay(long, TimeUnit)} denotes how long to wait
 *   between each retry. The default is 0MS.</li>
 * </ul>
 * These three can be combined in any combination, though without specifying
 * {@link #upTo(int)} the behavior is identical to not using this Rule.
 *
 * @see org.junit.Rule
 */
public class Retry implements TestRule {

  private static final Logger LOG = LoggerFactory.getLogger(Retry.class);

  public static Retry none() {
    return new Retry();
  }

  public static Retry of(
      final int retries,
      final Class<? extends Throwable> exception,
      final long delay,
      final TimeUnit unit) {
    final Retry retry = new Retry();
    retry.upTo(retries);
    retry.when(exception);
    retry.withDelay(delay, unit);
    return retry;
  }

  private Class<? extends Throwable> exception = Throwable.class;
  private int maxRetries = 0;
  private long delay = 0;
  private TimeUnit unit = TimeUnit.MILLISECONDS;

  private Retry() {}

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public void when(final Class<? extends Throwable> exception) {
    this.exception = exception;
  }

  public void upTo(final int retries) {
    this.maxRetries = retries;
  }

  public void withDelay(final long delay, final TimeUnit unit) {
    this.unit = unit;
    this.delay = delay;
  }

  @Override
  public Statement apply(final Statement base, final Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        int retries = 0;

        while (true) {
          try {
            base.evaluate();
            return;
          } catch (final Throwable e) {
            retries++;
            if (!(exception.isInstance(e) || exception.isInstance(ExceptionUtils.getRootCause(e)))
                || retries > maxRetries) {
              throw e;
            }
            LOG.warn("Retrying test after {} {} due to: {}", delay, unit, e.getMessage());
            unit.sleep(delay);
          }
        }
      }
    };
  }

}
