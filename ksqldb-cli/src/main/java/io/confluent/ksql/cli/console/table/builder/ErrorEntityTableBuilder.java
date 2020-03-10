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

package io.confluent.ksql.cli.console.table.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import io.confluent.ksql.cli.console.table.Table;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.rest.entity.ErrorEntity;
import java.io.IOException;

public class ErrorEntityTableBuilder implements TableBuilder<ErrorEntity> {

  private static final ObjectMapper MAPPER = JsonMapper.INSTANCE.mapper;

  @Override
  public Table buildTable(final ErrorEntity entity) {
    final String message = entity.getErrorMessage();

    String formatted;
    try {
      formatted = MAPPER
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(MAPPER.readTree(message));
    } catch (final IOException e) {
      formatted = String.join("\n", Splitter.fixedLength(60).splitToList(message));
    }

    return new Table.Builder()
        .withColumnHeaders("Error")
        .withRow(formatted)
        .build();
  }
}
