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

package io.confluent.ksql.function.udaf;

import io.confluent.ksql.function.KsqlTableFunction;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.connect.data.Schema;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
/*
 * Used during when creating UDTFS from the ext dir
 */
public interface UdtfArgSupplier {

  KsqlTableFunction apply(List<Schema> args,
      Schema outputType,
      Optional<Metrics> metrics);
}
