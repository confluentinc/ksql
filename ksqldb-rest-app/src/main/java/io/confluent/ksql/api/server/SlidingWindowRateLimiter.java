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

package io.confluent.ksql.api.server;

import static io.confluent.ksql.util.KsqlPreconditions.checkArgument;

import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.util.LinkedList;
import java.util.Queue;

public class SlidingWindowRateLimiter {
  private static long NUM_BYTES_IN_ONE_MEGABYTE = 1048576;
  private final Queue<Pair<Long, Long>> queue;  // <timestamp, bytes>
  private final long requestLimit;       //MEASURED IN BYTES
  private final long timeLimit;  //1 hour in miliseconds
  private long lastHourBytes;

  public SlidingWindowRateLimiter(final int requestLimitInMB, final long timeLimit) {
    checkArgument(requestLimitInMB >= 0,
            "Pull Query bandwidth limit can't be negative.");
    checkArgument(timeLimit >= 0,
            "Pull Query throttle window size can't be negative");

    this.requestLimit = (long) requestLimitInMB * NUM_BYTES_IN_ONE_MEGABYTE;
    this.timeLimit = timeLimit;
    this.queue = new LinkedList<>();
    this.lastHourBytes = 0;
  }

  public synchronized void allow(final long timestamp) throws KsqlException {
    checkArgument(timestamp >= 0,
            "Timestamp can't be negative.");

    while (!queue.isEmpty() && timestamp - queue.peek().left >= timeLimit) {
      this.lastHourBytes -= queue.poll().right;
    }
    if (this.lastHourBytes > requestLimit) {
      throw new KsqlException("Host is at bandwidth rate limit for pull queries.");
    }
  }

  public synchronized void add(final long timestamp, final long responseSizeInBytes) {
    checkArgument(timestamp >= 0,
            "Timestamp can't be negative.");
    checkArgument(responseSizeInBytes >= 0,
            "Response size can't be negative.");

    queue.add(new Pair<>(timestamp, responseSizeInBytes));
    this.lastHourBytes += responseSizeInBytes;
  }
}