/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.query;

import io.confluent.ksql.query.QueryError.Type;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The {@code RegexClassifier} classifies errors based on regex patterns of
 * the stack trace. This class is intended as "last resort" so that noisy
 * errors can be classified without deploying a new version of code.
 */
public final class RegexClassifier implements QueryErrorClassifier {

  private static final Logger LOG = LogManager.getLogger(RegexClassifier.class);

  private final Pattern pattern;
  private final Type type;
  private final String queryId;

  /**
   * Specifying a RegexClassifier in a config requires specifying
   * {@code <TYPE> <PATTERN>}, for example {@code USER .*InvalidTopicException.*}
   *
   * @param config the configuration specifying the type and pattern
   * @return the classifier
   */
  public static QueryErrorClassifier fromConfig(final String config, final String queryId) {
    final String[] split = config.split("\\s", 2);
    if (split.length < 2) {
      LOG.warn("Ignoring invalid configuration for RegexClassifier: " + config);
      return err -> Type.UNKNOWN;
    }

    return new RegexClassifier(
        Type.valueOf(split[0].toUpperCase()),
        Pattern.compile(split[1], Pattern.DOTALL),
        queryId
    );
  }

  private RegexClassifier(final Type type, final Pattern pattern, final String queryId) {
    this.type = Objects.requireNonNull(type, "type");
    this.pattern = Objects.requireNonNull(pattern, "pattern");
    this.queryId = Objects.requireNonNull(queryId, "queryId");
  }

  @Override
  public Type classify(final Throwable e) {
    LOG.info("Attempting to classify for {} under regex pattern {}.", queryId, pattern);

    Throwable error = e;
    do {
      if (matches(error)) {
        LOG.warn(
            "Classified error for queryId {} under regex pattern {} as type {}.",
            queryId,
            pattern,
            type);
        return type;
      }
      error = error.getCause();
    } while (error != null);

    return Type.UNKNOWN;
  }

  private boolean matches(final Throwable e) {
    final boolean clsMatches = pattern.matcher(e.getClass().getName()).matches();
    final boolean msgMatches = e.getMessage() != null && pattern.matcher(e.getMessage()).matches();
    return clsMatches || msgMatches;
  }

}
