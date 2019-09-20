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

package io.confluent.ksql.parser.properties.with;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.properties.with.ConfigMetaData;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.util.KsqlException;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;

/**
 * Base class for handling 'with clause' properties.
 */
@Immutable
abstract class WithClauseProperties extends AbstractConfig {

  private final ConfigMetaData configDetails;
  private final ImmutableMap<String, Literal> originalLiterals;

  WithClauseProperties(final ConfigMetaData configDetails, final Map<String, Literal> originals) {
    super(
        configDetails.getConfigDef(),
        toValues(
            configDetails.getShortConfigs(),
            Objects.requireNonNull(originals, "originals")
        ),
        false
    );

    throwOnUnknownProperty(configDetails.getConfigNames(), originals);

    this.configDetails = Objects.requireNonNull(configDetails, "configDetails");

    this.originalLiterals = ImmutableMap.copyOf(originals.entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey().toUpperCase(), Map.Entry::getValue)));
  }

  @Override
  public String toString() {
    return configDetails.getOrderedConfigNames().stream()
        .filter(originalLiterals::containsKey)
        .map(name -> name + "=" + originalLiterals.get(name))
        .collect(Collectors.joining(", "));
  }

  public Map<String, Literal> copyOfOriginalLiterals() {
    return new HashMap<>(originalLiterals);
  }

  void validateDateTimeFormat(final String configName) {
    final Object value = originals().get(configName);
    if (value == null) {
      return;
    }

    final String pattern = value.toString();

    try {
      DateTimeFormatter.ofPattern(pattern);
    } catch (final Exception e) {
      throw new KsqlException("Invalid datatime format for"
          + " config:" + configName
          + ", reason:" + e.getMessage(), e);
    }
  }

  private static Map<String, Object> toValues(
      final Set<String> shortConfigProperties,
      final Map<String, Literal> literals
  ) {
    final Map<String, Object> values = literals.entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey().toUpperCase(), e -> e.getValue().getValue()));

    shortConfigProperties.forEach(configName -> {
      final Object rf = values.get(configName);
      if (rf instanceof Number) {
        values.put(configName, ((Number) rf).shortValue());
      }
    });

    return values;
  }

  private static void throwOnUnknownProperty(
      final Set<String> configNames,
      final Map<String, ?> originals
  ) {
    final Set<String> providedNames = originals.keySet().stream()
        .map(String::toUpperCase)
        .collect(Collectors.toSet());

    final SetView<String> onlyInProvided = Sets.difference(providedNames, configNames);
    if (!onlyInProvided.isEmpty()) {
      throw new KsqlException("Invalid config variable(s) in the WITH clause: "
          + String.join(",", onlyInProvided));
    }
  }

  public Optional<Character> getValueDelimiter() {
    final String providedValueDelimiter = getString(CommonCreateConfigs.VALUE_DELIMITER_PROPERTY);
    if (providedValueDelimiter != null && providedValueDelimiter.equalsIgnoreCase("$")) {
      System.out.println("foo");
    }
    if (providedValueDelimiter != null) {
      if (providedValueDelimiter.isEmpty()) {
        throw new KsqlException("Error in WITH clause property '"
            + CommonCreateConfigs.VALUE_DELIMITER_PROPERTY
            + "': Delimiter cannot be empty or whitespace."
            + System.lineSeparator()
            + "For tab or space delimited use 'TAB' or 'SPACE' as the delimeter."
        );
      } else if (providedValueDelimiter.length() == 1) {
        return Optional.of(providedValueDelimiter.charAt(0));
      } else {
        Character delim = NAMED_DELIMITERS.get(providedValueDelimiter);
        if (delim != null) {
          return Optional.of(delim);
        } else {
          throw new KsqlException("Error in WITH clause property '"
              + CommonCreateConfigs.VALUE_DELIMITER_PROPERTY
              + "': Delimiter must be a single character, 'TAB' or 'SPACE'."
              + System.lineSeparator()
              + "Example valid value: ';'"
          );
        }
      }
    } else {
      return Optional.empty();
    }
  }

  private static final Map<String, Character> NAMED_DELIMITERS = ImmutableMap
      .<String, Character>builder()
      .put("TAB", '\t')
      .put("SPACE", ' ')
      .build();


}
