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

package io.confluent.ksql.version.metrics.collector;


/**
 * Types of Ksql Module.
 *
 * <p>Note: If you may any changes to this enum you will also need to make changes to the
 * {@link io.confluent.support.metrics.validate.KSqlValidModuleType} enum in support-metrics-common,
 * which stores the full list of module types that have ever been valid.
 */
public enum KsqlModuleType {
  CLI, SERVER
}
