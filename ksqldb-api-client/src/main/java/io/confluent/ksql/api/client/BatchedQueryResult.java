/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.client;

import java.util.List;

/**
 * The result of a query (push or pull), returned as a single batch once the query has finished
 * executing. For non-terminating push queries, {@code StreamedQueryResult} should be used instead.
 */
public interface BatchedQueryResult {

  List<String> columnNames();

  List<ColumnType> columnTypes();

  String queryID();

  List<Row> rows();
}
