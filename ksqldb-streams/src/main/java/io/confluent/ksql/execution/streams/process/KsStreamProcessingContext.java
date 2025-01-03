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

import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import org.apache.kafka.streams.processor.api.ProcessingContext;
import org.apache.kafka.streams.processor.api.Processor;

/**
 * this implements the KsqlProcessingContext interface using {@link ProcessingContext} from
 * {@link Processor} Api.
 *
 */
public class KsStreamProcessingContext implements KsqlProcessingContext {

  private final ProcessingContext context;

  public KsStreamProcessingContext(final ProcessingContext processingContext) {
    this.context = processingContext;
  }

  @Override
  public long getRowTime() {
    return context.currentStreamTimeMs();
  }

}
