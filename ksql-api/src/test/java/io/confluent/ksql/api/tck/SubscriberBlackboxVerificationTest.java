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
import org.reactivestreams.Subscriber;
import org.reactivestreams.tck.SubscriberBlackboxVerification;
import org.reactivestreams.tck.TestEnvironment;

/*
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class SubscriberBlackboxVerificationTest extends SubscriberBlackboxVerification<Buffer> {

  public SubscriberBlackboxVerificationTest() {
    super(new TestEnvironment());
  }

  @Override
  public Buffer createElement(int i) {
    return Buffer.buffer("element" + i);
  }

  @Override
  public Subscriber<Buffer> createSubscriber() {
    return null;
  }

  @Override
  public void triggerRequest(Subscriber<? super Buffer> subscriber) {

  }


}
