/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.execution.streams.process;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class KsProcessor<KInT, KOutT> implements Processor<KInT, GenericRow, KOutT, GenericRow> {
  private ProcessorContext<KOutT, GenericRow> processorContext;
  private final KsqlTransformer<KInT, KOutT> keyDelegate;
  private final KsqlTransformer<KInT, GenericRow> valueDelegate;

  public KsProcessor(final KsqlTransformer<KInT, KOutT> keyDelegate,
      final KsqlTransformer<KInT, GenericRow> valueDelegate) {
    this.keyDelegate = requireNonNull(keyDelegate, "keyDelegate");
    this.valueDelegate = requireNonNull(valueDelegate, "valueDelegate");
  }

  @Override
  public void init(final ProcessorContext<KOutT, GenericRow> processContext) {
    this.processorContext = processContext;
  }

  @Override
  public void process(final Record<KInT, GenericRow> record) {
    final KInT key = record.key();
    final GenericRow value = record.value();
    final KsqlProcessingContext context = new KsStreamProcessingContext(processorContext);
    final Record<KOutT, GenericRow> newRecord = new Record<>(
        keyDelegate.transform(key, value, context),
        valueDelegate.transform(key, value, context),
        record.timestamp()
    );
    processorContext.forward(newRecord);
  }

  @Override
  public void close() { }
}
