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

package io.confluent.ksql.physical;

import io.confluent.ksql.GenericRow;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

public class AddTimestampColumn implements ValueTransformerSupplier<GenericRow, GenericRow> {
  @Override
  public ValueTransformer<GenericRow, GenericRow> get() {
    return new ValueTransformer<GenericRow, GenericRow>() {
      ProcessorContext processorContext;
      @Override
      public void init(final ProcessorContext processorContext) {
        this.processorContext = processorContext;
      }

      @Override
      public GenericRow transform(final GenericRow row) {
        if (row != null) {
          row.getColumns().add(0, processorContext.timestamp());
        }
        return row;
      }

      @Override
      public void close() {
      }
    };
  }
}
