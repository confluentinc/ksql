/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.scalablepush.operators;

import io.confluent.ksql.execution.scalablepush.ScalablePushRegistry;

/**
 * Represents a data source for a scalable push query
 */
public interface PushDataSourceOperator {
  ScalablePushRegistry getScalablePushRegistry();

  // Since push queries are pushed and not pulled, it needs to call back on the operator to tell it
  // when data can be read.
  void setNewRowCallback(Runnable newRowCallback);

  // If rows have been dropped.
  boolean droppedRows();

  // If an error has occurred.
  boolean hasError();

  // Number of rows read
  long getRowsReadCount();
}
