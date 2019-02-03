/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.function;

import io.confluent.ksql.function.udaf.TableUdaf;
import java.util.List;
import java.util.function.Supplier;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.connect.data.Schema;

@SuppressWarnings("unused") // used in generated code
public abstract class GeneratedTableAggregateFunction
    extends GeneratedAggregateFunction implements TableAggregationFunction {

  public GeneratedTableAggregateFunction(final String functionName,
                                         final Schema returnType,
                                         final List<Schema> arguments,
                                         final String description) {
    super(functionName, returnType, arguments, description);
  }

  protected GeneratedTableAggregateFunction(final String functionName, final int udafIndex,
                                            final Supplier udafSupplier,
                                            final Schema returnType,
                                            final List<Schema> arguments,
                                            final String description,
                                            final Sensor aggregateSensor,
                                            final Sensor mergeSensor) {
    super(functionName, udafIndex, udafSupplier, returnType, arguments, description,
          aggregateSensor,
          mergeSensor);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object undo(final Object valueToUndo, final Object aggregateValue) {
    return ((TableUdaf) getUdaf()).undo(valueToUndo, aggregateValue);
  }
}
