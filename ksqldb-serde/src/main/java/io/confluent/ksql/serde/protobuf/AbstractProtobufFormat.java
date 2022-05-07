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

package io.confluent.ksql.serde.protobuf;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.ksql.serde.connect.ConnectFormat;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class contains common method from {@link ProtobufFormat} and {@link ProtobufNoSRFormat}
 */
public abstract class AbstractProtobufFormat  extends ConnectFormat {

  @Override
  public List<String> schemaFullNames(final ParsedSchema schema) {
    if (schema.rawSchema() instanceof ProtoFileElement) {
      final ProtoFileElement protoFileElement = (ProtoFileElement) schema.rawSchema();
      final String packageName = protoFileElement.getPackageName();

      return protoFileElement.getTypes().stream()
          .map(typeElement -> Joiner.on(".").skipNulls().join(packageName, typeElement.getName()))
          .collect(Collectors.toList());
    }

    return ImmutableList.of();
  }
}
