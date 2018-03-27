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

public class TokenBucket {
  private int capacity;
  private int rate;
  private int level;
  private Object lock;
  private long lastFill;

  public TokenBucket(int capacity, int rate) {
    this.rate = rate;
    this.capacity = capacity;
    this.lock = new Object();
    level = capacity;
    lastFill = System.currentTimeMillis();
  }

  public int take(int quantity) throws InterruptedException {
    while (true) {
      long now = System.currentTimeMillis();
      long nextFill;

      synchronized (this.lock) {
        int elapsed = (int) ((now - lastFill) / 1000);
        if (elapsed > 0) {
          this.level += elapsed * this.rate;
          this.level = Math.min(this.level, this.capacity);
          this.lastFill = this.lastFill + elapsed * 1000;
        }
        if (this.level > 0) {
          quantity = Math.min(quantity, this.level);
          this.level -= quantity;
          return quantity;
        }

        nextFill = this.lastFill + 1000;
      }

      now = System.currentTimeMillis();
      if (now < nextFill) {
        Thread.sleep(nextFill - now);
      }
    }
  }
}
