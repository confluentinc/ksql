/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.parser;

/**
 * Controls how the result of a query is materialized.
 */
public enum OutputRefinement {

  /**
   * All intermediate results should be materialized.
   *
   * <p>For a table, this would mean output all the events in the change log, or all the
   * intermediate aggregation results being computed in an aggregating query.
   *
   * <p>For a stream, all events are changes, so all are output.
   */
  CHANGES,

  /**
   * Only final results should be materialized.
   *
   * <p>For a table, this would mean output only the finalized result per-key.
   *
   * <p>For a stream, all events are final, so all are output.
   */
  FINAL
}


