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

package io.confluent.ksql.function;

import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.List;
import org.apache.kafka.connect.data.Schema;

/**
 * A wrapper around the actual table function which provides methods to get return type and
 * description, and allows the function to be invoked.
 */
public interface KsqlTableFunction extends FunctionSignature {

  Schema getReturnType();

  SqlType returnType();

  List<?> apply(Object... args);

  String getDescription();
}
