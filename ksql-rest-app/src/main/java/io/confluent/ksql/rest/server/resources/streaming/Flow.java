/**
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
 **/

package io.confluent.ksql.rest.server.resources.streaming;

import org.apache.kafka.connect.data.Schema;

/**
 * Flow constructs borrowed from Java 9
 * https://docs.oracle.com/javase/9/docs/api/java/util/concurrent/Flow.Subscription.html
 */
public class Flow {

  public interface Subscriber<T> {

    void onNext(T item);

    void onError(Throwable e);

    void onComplete();

    /**
     * this method is an addition to the Java 9 API
     *
     * <p>onSchema may be called right before the first onNext call
     * to describe the message format, in case the stream has a schema
     * @param schema schema for upcoming messages
     */
    void onSchema(Schema schema);

    void onSubscribe(Subscription subscription);
  }

  public interface Subscription {

    void cancel();

    void request(long n);
  }

  public interface Publisher<T> {

    void subscribe(Subscriber<T> subscriber);
  }
}
