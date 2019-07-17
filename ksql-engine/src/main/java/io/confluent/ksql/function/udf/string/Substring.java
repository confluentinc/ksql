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

package io.confluent.ksql.function.udf.string;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.Configurable;

@SuppressWarnings("unused") // Invoked via reflection.
@UdfDescription(name = "substring",
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Returns a substring of the passed in value.\n"
        + "The behaviour of this function changed in release 5.1. "
        + "It is possible to switch the function back to pre-v5.1 functionality via the setting:\n"
        + "\t" + KsqlConfig.KSQL_FUNCTIONS_SUBSTRING_LEGACY_ARGS_CONFIG + "\n"
        + "This can be set globally, through the server configuration file, "
        + "or per sessions or query via the set command.")
public class Substring implements Configurable {

  private Impl impl = new CurrentImpl();

  @Override
  public void configure(final Map<String, ?> props) {
    final KsqlConfig config = new KsqlConfig(props);
    final boolean legacyArgs =
        config.getBoolean(KsqlConfig.KSQL_FUNCTIONS_SUBSTRING_LEGACY_ARGS_CONFIG);

    impl = legacyArgs ? new LegacyImpl() : new CurrentImpl();
  }

  @Udf(description = "Returns a substring of str that starts at pos"
      + " and continues to the end of the string")
  public String substring(
      @UdfParameter(
          description = "The source string. If null, then function returns null.") final String str,
      @UdfParameter(
          description = "The base-one position the substring starts from."
              + " If null, then function returns null."
              + " (If in legacy mode, this argument is base-zero)") final Integer pos) {
    return impl.substring(str, pos);
  }

  @Udf(description = "Returns a substring of str that starts at pos and is of length len")
  public String substring(
      @UdfParameter(
          description = "The source string. If null, then function returns null.") final String str,
      @UdfParameter(
          description = "The base-one position the substring starts from."
              + " If null, then function returns null."
              + " (If in legacy mode, this argument is base-zero)") final Integer pos,
      @UdfParameter(
          description = "The length of the substring to extract."
              + " If null, then function returns null."
              + " (If in legacy mode, this argument is the endIndex (exclusive),"
              + " rather than the length") final Integer length) {
    return impl.substring(str, pos, length);
  }

  private interface Impl {
    String substring(String value, Integer pos);

    String substring(String value, Integer pos, Integer length);
  }

  private static final class LegacyImpl implements Impl {
    public String substring(final String value, final Integer startIndex) {
      Objects.requireNonNull(startIndex, "startIndex");
      return value.substring(startIndex);
    }

    public String substring(final String value, final Integer startIndex, final Integer endIndex) {
      Objects.requireNonNull(startIndex, "startIndex");
      Objects.requireNonNull(endIndex, "endIndex");
      return value.substring(startIndex, endIndex);
    }
  }

  private static final class CurrentImpl implements Impl {
    public String substring(final String str, final Integer pos) {
      if (str == null || pos == null) {
        return null;
      }
      final int start = getStartIndex(str, pos);
      return str.substring(start);
    }

    public String substring(final String str, final Integer pos, final Integer length) {
      if (str == null || pos == null || length == null) {
        return null;
      }
      final int start = getStartIndex(str, pos);
      final int end = getEndIndex(str, start, length);
      return str.substring(start, end);
    }

    private static int getStartIndex(final String value, final Integer pos) {
      return pos < 0
          ? Math.max(value.length() + pos, 0)
          : Math.max(Math.min(pos - 1, value.length()), 0);
    }

    private static int getEndIndex(final String value, final int start, final int length) {
      return Math.max(Math.min(start + length, value.length()), start);
    }
  }
}
