/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the
 * License.
 */

package io.confluent.ksql.function.udf.string;

import com.google.common.base.Splitter;
import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@UdfDescription(
    name = "split_to_map",
    category = FunctionCategory.STRING,
    description = "Splits a string into key-value pairs and creates a map from them. The "
        + "'entryDelimiter' splits the string into key-value pairs which are then split by "
        + "'kvDelimiter'. If the same key is present multiple times in the input, the latest "
        + "value for that key is returned. Returns NULL if the input text or either of the "
        + "delimiters is NULL.")
public class SplitToMap {
  @Udf
  public Map<String, String> splitToMap(
      @UdfParameter(
          description = "Separator string and values to join") final String input,
      @UdfParameter(
          description = "Separator string and values to join") final String entryDelimiter,
      @UdfParameter(
          description = "Separator string and values to join") final String kvDelimiter) {

    if (input == null || entryDelimiter == null || kvDelimiter == null) {
      return null;
    }

    if (entryDelimiter.isEmpty() || kvDelimiter.isEmpty() || entryDelimiter.equals(kvDelimiter)) {
      return null;
    }

    final Iterable<String> entries = Splitter.on(entryDelimiter).omitEmptyStrings().split(input);
    return StreamSupport.stream(entries.spliterator(), false)
        .filter(e -> e.contains(kvDelimiter))
        .map(kv -> Splitter.on(kvDelimiter).split(kv).iterator())
        .collect(Collectors.toMap(
            Iterator::next,
            Iterator::next,
            (v1, v2) -> v2));
  }
}
