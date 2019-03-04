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

package io.confluent.ksql.properties;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.util.KsqlException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Utility class for working with property files and system properties.
 */
public final class PropertiesUtil {

  private static final Set<Predicate<String>> BLACK_LIST = ImmutableSet
      .<Predicate<String>>builder()
      .add(key -> key.startsWith("java."))
      .add(key -> key.startsWith("os."))
      .add(key -> key.startsWith("sun."))
      .add(key -> key.startsWith("user."))
      .add(key -> key.startsWith("line.separator"))
      .add(key -> key.startsWith("path.separator"))
      .add(key -> key.startsWith("file.separator"))
      .build();

  private static final Predicate<String> IS_BLACKLISTED = BLACK_LIST.stream()
      .reduce(key -> false, Predicate::or);

  private static final Predicate<String> NOT_BLACKLISTED = IS_BLACKLISTED.negate();

  private PropertiesUtil() {
  }

  /**
   * Convert the supplied map props to an old school {@code Properties} instance.
   *
   * @param mapProps the map props to convert
   * @return {@code Properties} instance.
   */
  public static Properties asProperties(final Map<String, String> mapProps) {
    final Properties properties = new Properties();
    properties.putAll(mapProps);
    return properties;
  }

  /**
   * Load a property file.
   *
   * @param propertiesFile the property file to load.
   * @return an immutable map of the loaded properties.
   */
  public static Map<String, String> loadProperties(final File propertiesFile) {
    final Map<String, String> properties = loadPropsFromFile(propertiesFile);
    throwOnBlackListedProperties(properties);
    return properties;
  }

  /**
   * Apply non-blacklisted entries in the suplied {@code overrides} to the supplied {@code props}.
   *
   * @param props the props to overwrite with sys props.
   * @return an immutable map of merged props.
   */
  public static Map<String, String> applyOverrides(
      final Map<String, String> props,
      final Properties overrides
  ) {
    final Map<String, String> overridesMap = asMap(overrides);

    final HashMap<String, String> merged = new HashMap<>(props);
    merged.putAll(filterByKey(overridesMap, NOT_BLACKLISTED));

    return ImmutableMap.copyOf(merged);
  }

  /**
   * Remove any properties where the key does not pass the supplied {@code predicate}.
   *
   * @param props the props to filter
   * @param predicate the key predicate
   * @return the filtered props.
   */
  public static Map<String, String> filterByKey(
      final Map<String, String> props,
      final Predicate<String> predicate
  ) {
    final Builder<String, String> builder = ImmutableMap.builder();

    props.entrySet().stream()
        .filter(e -> predicate.test(e.getKey()))
        .forEach(e -> builder.put(e.getKey(), e.getValue()));

    return builder.build();
  }

  private static Map<String, String> loadPropsFromFile(final File propertiesFile) {
    final Properties properties = new Properties();
    try (FileInputStream inputStream = new FileInputStream(propertiesFile)) {
      properties.load(inputStream);
    } catch (final IOException e) {
      throw new KsqlException("Failed to load properties file: " + propertiesFile);
    }

    return asMap(properties);
  }

  private static void throwOnBlackListedProperties(final Map<String, ?> properties) {
    final String separator = System.lineSeparator() + "\t- ";

    final String blacklisted = properties.keySet().stream()
        .filter(IS_BLACKLISTED)
        .collect(Collectors.joining(separator));

    if (!blacklisted.isEmpty()) {
      throw new KsqlException("Property file contains the following blacklisted properties "
          + "(Please remove them an try again):"
          + separator + blacklisted);
    }
  }

  private static Map<String, String> asMap(final Properties props) {
    final Builder<String, String> builder = ImmutableMap.builder();
    props.stringPropertyNames().forEach(key -> builder.put(key, props.getProperty(key)));
    return builder.build();
  }
}
