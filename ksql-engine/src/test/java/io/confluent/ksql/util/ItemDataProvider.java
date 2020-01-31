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

package io.confluent.ksql.util;

import static io.confluent.ksql.GenericRow.genericRow;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeOption;
import java.util.Map;

public class ItemDataProvider extends TestDataProvider<String> {

  private static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("ID"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("DESCRIPTION"), SqlTypes.STRING)
      .build();
  private static final PhysicalSchema PHYSICAL_SCHEMA = PhysicalSchema
      .from(LOGICAL_SCHEMA, SerdeOption.none());

  private static final Map<String, GenericRow> ROWS = ImmutableMap.<String, GenericRow>builder()
      .put("ITEM_1", genericRow("ITEM_1", "home cinema"))
      .put("ITEM_2", genericRow("ITEM_2", "clock radio"))
      .put("ITEM_3", genericRow("ITEM_3", "road bike"))
      .put("ITEM_4", genericRow("ITEM_4", "mountain bike"))
      .put("ITEM_5", genericRow("ITEM_5", "snowboard"))
      .put("ITEM_6", genericRow("ITEM_6", "iphone 10"))
      .put("ITEM_7", genericRow("ITEM_7", "gopro"))
      .put("ITEM_8", genericRow("ITEM_8", "cat"))
      .build();

  public ItemDataProvider() {
    super("ITEM", "ID", PHYSICAL_SCHEMA, ROWS);
  }
}
