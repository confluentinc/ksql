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

package io.confluent.ksql.rest.server;

import io.confluent.rest.Application;
import io.confluent.rest.RestConfig;

/**
 * A strongly typed class indicating that the implementing class is an {@code Application}
 * that implements {@code Executable}.
 */
public abstract class ExecutableApplication<T extends RestConfig>
    extends Application<T>
    implements Executable {

  public ExecutableApplication(final T config) {
    super(config);
  }

  public ExecutableApplication(final T config, final String path) {
    super(config, path);
  }
}
