/*
 * Copyright 2014 Red Hat, Inc.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  and Apache License v2.0 which accompanies this distribution.
 *
 *  The Eclipse Public License is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 *
 *  The Apache License v2.0 is available at
 *  http://www.opensource.org/licenses/apache2.0.php
 *
 *  You may elect to redistribute this code under either of these licenses.
 */

package io.confluent.ksql.api.tck;

import io.vertx.core.buffer.Buffer;
import org.junit.Ignore;
import org.reactivestreams.Subscriber;
import org.reactivestreams.tck.SubscriberWhiteboxVerification;
import org.reactivestreams.tck.TestEnvironment;

/*
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
@Ignore
public class SubscriberWhiteboxVerificationTest extends SubscriberWhiteboxVerification<Buffer> {


  public SubscriberWhiteboxVerificationTest() {
    super(new TestEnvironment());
  }

  @Override
  public Buffer createElement(final int element) {
    return null;
  }

  @Override
  public Subscriber<Buffer> createSubscriber(final WhiteboxSubscriberProbe<Buffer> probe) {
    return null;
  }
}
