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

package io.confluent.ksql.physical.scalablepush;

import io.confluent.ksql.physical.scalablepush.PushRouting.PushConnectionsHandle;
import java.util.concurrent.CompletableFuture;

/**
 * Populates the result queue for scalable push queries.
 */
public interface PushQueryQueuePopulator {

  CompletableFuture<PushConnectionsHandle> run();
}
