/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.physical;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

import io.confluent.ksql.GenericRow;

public class AddTimestampColumn implements ValueTransformerSupplier<GenericRow, GenericRow> {
  @Override
  public ValueTransformer<GenericRow, GenericRow> get() {
    return new ValueTransformer<GenericRow, GenericRow>() {
      ProcessorContext processorContext;
      @Override
      public void init(ProcessorContext processorContext) {
        this.processorContext = processorContext;
      }

      @Override
      public GenericRow transform(GenericRow row) {
        if (row != null) {
          row.getColumns().add(0, processorContext.timestamp());
        }
        return row;
      }

      @Override
      public GenericRow punctuate(long l) {
        return null;
      }

      @Override
      public void close() {
      }
    };
  }
}
