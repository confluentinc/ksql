/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.context;

import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.util.KsqlConfig;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

/**
 * Abstract class to handle KSQL user impersonation. Subclass implementation needs to implement
 * the {@link AbstractBinder} methods and configure the binder.
 * </p>
 * The subclass implementation must be registered with the
 * {@link KsqlRestConfig#KSQL_IMPERSONATION_HANDLER_CLASS} configuration.
 */
public abstract class KsqlImpersonationHandler extends AbstractBinder {
  protected final KsqlConfig ksqlConfig;

  public KsqlImpersonationHandler(final KsqlConfig ksqlConfig) {
    this.ksqlConfig = ksqlConfig;
  }
}
