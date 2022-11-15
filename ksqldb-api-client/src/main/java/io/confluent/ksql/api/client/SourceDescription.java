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
import java.util.Optional;

/**
 * Metadata for a ksqlDB stream or table.
 */
public interface SourceDescription {

  /**
   * @return name of this stream/table
   */
  String name();

  /**
   * @return type of this source, i.e., whether this source is a stream or table
   */
  String type();

  /**
   * @return list of fields (key and value) present in this stream/table
   */
  List<FieldInfo> fields();

  /**
   * @return name of the Kafka topic underlying this ksqlDB stream/table
   */
  String topic();

  /**
   * @return key serialization format of the data in this stream/table
   */
  String keyFormat();

  /**
   * @return value serialization format of the data in this stream/table
   */
  String valueFormat();

  /**
   * @return list of ksqlDB queries currently reading from this stream/table
   */
  List<QueryInfo> readQueries();

  /**
   * @return list of ksqlDB queries currently writing to this stream/table
   */
  List<QueryInfo> writeQueries();

  /**
   * @return name of the column configured as the {@code TIMESTAMP} for this stream/table, if any
   */
  Optional<String> timestampColumn();

  /**
   * Returns the type of the window (e.g., "TUMBLING", "HOPPING", "SESSION") associated with this
   * source, if this source is a windowed table. Else, empty.
   *
   * @return type of the window, if applicable
   */
  Optional<String> windowType();

  /**
   * Returns the ksqlDB statement text used to create this stream/table. This text may not be
   * exactly the statement submitted in order to create this stream/table, but submitting this
   * statement will result in exactly this stream/table being created.
   *
   * @return the ksqlDB statement text
   */
  String sqlStatement();

}
