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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.process.KsqlProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

public class KsProcessorSupplier<KInT, KOutT>
    implements ProcessorSupplier<KInT, GenericRow, KOutT, GenericRow> {

  private final KsProcessor<KInT, KOutT> processor;

  public KsProcessorSupplier(
      final KsqlProcessor<KInT, KOutT> keyDelegate,
      final KsqlProcessor<KInT, GenericRow> valueDelegate) {
    this.processor = new KsProcessor<>(keyDelegate, valueDelegate);
  }

  @Override
  public Processor<KInT, GenericRow, KOutT, GenericRow> get() {
    return processor;
  }
}
