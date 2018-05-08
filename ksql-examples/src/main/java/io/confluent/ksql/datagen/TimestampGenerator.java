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

package io.confluent.ksql.datagen;

import java.util.Random;

public class TimestampGenerator {
  private long next;
  private long burstCount;
  private long increment;
  private long burst;
  private Random random;

  TimestampGenerator(long start, long increment, long burst) {
    this.next = start;
    this.increment = increment;
    this.burst = burst;
    this.burstCount = 0;
    this.random = new Random();
  }

  public long next() {
    if (burstCount < burst) {
      burstCount += 1;
      return next;
    }
    burstCount = 0;
    next = next + (long)(increment * random.nextDouble());
    return next();
  }
}
