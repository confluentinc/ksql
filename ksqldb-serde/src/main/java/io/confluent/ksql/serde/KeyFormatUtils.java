/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.serde;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;

public final class KeyFormatUtils {

  private static final List<Format> SUPPORTED_KEY_FORMATS = ImmutableList.of(
      FormatFactory.NONE,
      FormatFactory.KAFKA,
      FormatFactory.DELIMITED,
      FormatFactory.JSON
  );

  private static final List<Format> KEY_FORMATS_UNDER_DEVELOPMENT = ImmutableList.of(
      FormatFactory.AVRO,
      FormatFactory.JSON_SR
  );

  /**
   * Until the primitive key work is complete, not all formats are supported as key formats.
   * Once complete, this method may be removed.
   *
   * @return whether or not this format is supported for keys
   */
  public static boolean isSupportedKeyFormat(final KsqlConfig config, final Format format) {
    if (SUPPORTED_KEY_FORMATS.contains(format)) {
      return true;
    }

    return config.getBoolean(KsqlConfig.KSQL_KEY_FORMAT_ENABLED)
        && KEY_FORMATS_UNDER_DEVELOPMENT.contains(format);
  }

  private KeyFormatUtils() {
  }
}
