/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.plan;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import java.util.Set;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = As.PROPERTY
)
@JsonSubTypes({
    @Type(value = FormatsV1.class, name = "formatsV1")
})
public interface Formats {
  KeyFormat getKeyFormat();

  ValueFormat getValueFormat();

  Set<SerdeOption> getOptions();

  static Formats of(
      final KeyFormat keyFormat,
      final ValueFormat valueFormat,
      final Set<SerdeOption> options) {
    return new FormatsV1(keyFormat, valueFormat, options);
  }
}
