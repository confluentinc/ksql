/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.structured;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.TableAggregationFunction;
import io.confluent.ksql.function.udaf.KudafAggregator;
import io.confluent.ksql.function.udaf.KudafUndoAggregator;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.streams.MaterializedFactory;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

public class SchemaKGroupedTable extends SchemaKGroupedStream {
  private final KGroupedTable kgroupedTable;

  SchemaKGroupedTable(
      final Schema schema,
      final KGroupedTable kgroupedTable,
      final Field keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry
  ) {
    this(
        schema,
        kgroupedTable,
        keyField,
        sourceSchemaKStreams,
        ksqlConfig,
        functionRegistry,
        MaterializedFactory.create(ksqlConfig));
  }

  SchemaKGroupedTable(
      final Schema schema,
      final KGroupedTable kgroupedTable,
      final Field keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final MaterializedFactory materializedFactory
  ) {
    super(schema, null, keyField, sourceSchemaKStreams,
        ksqlConfig, functionRegistry, materializedFactory);

    this.kgroupedTable = Objects.requireNonNull(kgroupedTable, "kgroupedTable");
  }

  @SuppressWarnings("unchecked")
  @Override
  public SchemaKTable<String> aggregate(
      final Initializer initializer,
      final Map<Integer, KsqlAggregateFunction> aggValToFunctionMap,
      final Map<Integer, Integer> aggValToValColumnMap,
      final WindowExpression windowExpression,
      final Serde<GenericRow> topicValueSerDe,
      final String opName) {
    if (windowExpression != null) {
      throw new KsqlException("Windowing not supported for table aggregations.");
    }

    final List<String> unsupportedFunctionNames = aggValToFunctionMap.values()
        .stream()
        .filter(function -> !(function instanceof TableAggregationFunction))
        .map(KsqlAggregateFunction::getFunctionName)
        .collect(Collectors.toList());
    if (!unsupportedFunctionNames.isEmpty()) {
      throw new KsqlException(
          String.format(
            "The aggregation function(s) (%s) cannot be applied to a table.",
            String.join(", ", unsupportedFunctionNames)));
    }

    final KudafAggregator aggregator = new KudafAggregator(
        aggValToFunctionMap, aggValToValColumnMap);
    final Map<Integer, TableAggregationFunction> aggValToUndoFunctionMap =
        aggValToFunctionMap.keySet()
            .stream()
            .collect(
                Collectors.toMap(
                    k -> k,
                    k -> ((TableAggregationFunction) aggValToFunctionMap.get(k))));
    final KudafUndoAggregator subtractor = new KudafUndoAggregator(
        aggValToUndoFunctionMap, aggValToValColumnMap);
    final Materialized<String, GenericRow, ?> materialized =
        materializedFactory.create(Serdes.String(), topicValueSerDe, opName);
    final KTable<String, GenericRow> aggKtable = kgroupedTable.aggregate(
        initializer,
        aggregator,
        subtractor,
        materialized);
    return new SchemaKTable<>(
        schema,
        aggKtable,
        keyField,
        sourceSchemaKStreams,
        Serdes.String(),
        SchemaKStream.Type.AGGREGATE,
        ksqlConfig,
        functionRegistry
    );
  }
}
