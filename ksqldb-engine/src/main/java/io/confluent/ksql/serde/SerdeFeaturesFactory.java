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

package io.confluent.ksql.serde;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.serde.json.JsonFormat;
import io.confluent.ksql.serde.kafka.KafkaFormat;
import io.confluent.ksql.serde.none.NoneFormat;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Optional;

public final class SerdeFeaturesFactory {

  private SerdeFeaturesFactory() {
  }

  /**
   * Build serde options for internal topics.
   *
   * <p>Internal topics don't normally need any serde features set, as they use the format
   * defaults.  However, until ksqlDB supports wrapped single keys, any internal topic with a key
   * format that supports both wrapping and unwrapping needs to have an explicit {@link
   * SerdeFeature#UNWRAP_SINGLES} set to ensure backwards compatibility is easily achievable once
   * wrapped keys are supported.
   *
   * <p>Note: The unwrap feature should only be set when there is only a single key column. As
   * ksql does not yet support multiple key columns, the only time there is no a single key column
   * is when there is no key column, i.e. key-less streams. Internal topics, i.e. changelog and
   * repartition topics, are never key-less. Hence this method can safely set the unwrap feature
   * without checking the schema.
   *
   * <p>The code that sets the feature can be removed once wrapped keys are supported. Issue 6296
   * tracks the removal.
   *
   * @param keyFormat the key format.
   * @return the options
   * @see <a href=https://github.com/confluentinc/ksql/issues/6296>Issue 6296</a>
   * @see InternalFormats#of
   */
  public static SerdeFeatures buildInternal(final Format keyFormat) {
    final ImmutableSet.Builder<SerdeFeature> builder = ImmutableSet.builder();

    getKeyWrapping(true, keyFormat)
        .ifPresent(builder::add);

    return SerdeFeatures.from(builder.build());
  }

  public static SerdeFeatures buildKeyFeatures(
      final LogicalSchema schema,
      final Format keyFormat
  ) {
    return buildKeyFeatures(keyFormat, schema.key().size() == 1);
  }

  public static SerdeFeatures buildKeyFeatures(
      final Format keyFormat,
      final boolean isSingleKey
  ) {
    final ImmutableSet.Builder<SerdeFeature> builder = ImmutableSet.builder();

    getKeyWrapping(isSingleKey, keyFormat)
        .ifPresent(builder::add);

    return SerdeFeatures.from(builder.build());
  }

  public static SerdeFeatures buildValueFeatures(
      final LogicalSchema schema,
      final Format valueFormat,
      final SerdeFeatures explicitFeatures,
      final KsqlConfig ksqlConfig
  ) {
    final boolean singleColumn = schema.value().size() == 1;

    final ImmutableSet.Builder<SerdeFeature> builder = ImmutableSet.builder();

    getValueWrapping(singleColumn, valueFormat, explicitFeatures, ksqlConfig)
        .ifPresent(builder::add);

    return SerdeFeatures.from(builder.build());
  }

  /**
   * Not all {@code KeyFormat}s are valid internal topic formats. Specifically,
   * we want to ensure that the key format explicitly sets the wrapping if it
   * contains only a single column and is primitive. This method ensures
   * that key wrapping is eagerly set.
   *
   * <p>Additionally, internal topics with multiple key columns cannot use the NONE or KAFKA
   * key formats. In these cases, we switch to the JSON key format instead.
   *
   * <p>Note that while it is safe to call this method multiple times (it is idempotent),
   * it should be called before we build the execution steps to make sure that we never
   * serialize an execution step with a single key column and no wrapping serde features.
   * To achieve this, we've audited the SchemaKStream, SchemaKTable and SchemaGroupedKStream
   * classes to ensure that anytime a key is changed we properly set the key format.
   *
   * @param keyFormat un-sanitized format
   * @param newKeyColumnSqlTypes sql types of key columns for this stream/table
   * @param allowKeyFormatChangeToSupportNewKeySchema safeguard to prevent changing key formats in
   *                                                  unexpected ways. if false, no format change
   *                                                  will take place
   * @return the new key format to use
   */
  public static KeyFormat sanitizeKeyFormat(
      final KeyFormat keyFormat,
      final List<SqlType> newKeyColumnSqlTypes,
      final boolean allowKeyFormatChangeToSupportNewKeySchema
  ) {
    return sanitizeKeyFormatWrapping(
        !allowKeyFormatChangeToSupportNewKeySchema ? keyFormat :
        sanitizeKeyFormatForTypeCompatibility(
            sanitizeKeyFormatForMultipleColumns(
                keyFormat,
                newKeyColumnSqlTypes.size()),
            newKeyColumnSqlTypes
        ),
        newKeyColumnSqlTypes.size() == 1
    );
  }

  /**
   * Check whether given key format supports multiple columns.
   *
   * @param keyFormat the key format to be checked
   * @param numKeyColumns number of output key columns
   * @return Original key format if multiple column key types
   *         are supported, or else return the JSON format
   */
  private static KeyFormat sanitizeKeyFormatForMultipleColumns(
      final KeyFormat keyFormat,
      final int numKeyColumns
  ) {
    if (numKeyColumns <= 1 || formatSupportsMultipleColumns(keyFormat)) {
      return keyFormat;
    }
    return convertToJsonFormat(keyFormat);
  }

  /**
   * Check whether given sql type set could be supported by given key format.
   *
   * @param keyFormat the key format to be checked
   * @param sqlTypes the sql types to be tested
   * @return Original key format if multiple column key types
   *         are supported, or else return the JSON format
   */
  private static KeyFormat sanitizeKeyFormatForTypeCompatibility(final KeyFormat keyFormat,
                                                                 final List<SqlType> sqlTypes) {

    return sqlTypes.stream().allMatch(sqlType -> FormatFactory.of(keyFormat.getFormatInfo())
        .supportsKeyType(sqlType)) ? keyFormat : convertToJsonFormat(keyFormat);
  }

  private static KeyFormat convertToJsonFormat(final KeyFormat keyFormat) {
    if (keyFormat.isWindowed()) {
      throw new IllegalStateException("Should not convert format of windowed key");
    }

    return KeyFormat.nonWindowed(FormatInfo.of(JsonFormat.NAME), SerdeFeatures.of());
  }

  private static KeyFormat sanitizeKeyFormatWrapping(
      final KeyFormat keyFormat,
      final boolean isSingleKey
  ) {
    final Optional<SerdeFeature> keyWrapping = keyFormat
        .getFeatures()
        .findAny(SerdeFeatures.WRAPPING_FEATURES);
    final boolean hasWrappingFeature = keyWrapping.isPresent();

    // it is possible that the source format was either multi-key
    // or no-key, in which case there would not have been a wrapping
    // configuration specified - we should specify one here
    if (isSingleKey && !hasWrappingFeature) {
      final SerdeFeatures defaultWrapping =
          getKeyWrapping(true, FormatFactory.of(keyFormat.getFormatInfo()))
              .map(SerdeFeatures::of)
              .orElse(SerdeFeatures.of());
      return keyFormat.withSerdeFeatures(defaultWrapping);
    }

    // it is also possible that the source format was single-key and had a wrapping
    // configuration specified, but the new format is not single-key and therefore
    // should not have a wrapping configuration specified - we remove it here
    if (!isSingleKey && hasWrappingFeature) {
      return keyFormat.withoutSerdeFeatures(SerdeFeatures.of(keyWrapping.get()));
    }

    return keyFormat;
  }

  private static Optional<SerdeFeature> getKeyWrapping(
      final boolean singleKey,
      final Format keyFormat
  ) {
    // Until ksqlDB supports WRAP_SINGLE_KEYS in the WITH clause, we explicitly set
    // UNWRAP_SINGLE_KEYS for any key format that supports both wrapping and unwrapping to avoid
    // ambiguity later:
    if (singleKey
        && keyFormat.supportsFeature(SerdeFeature.UNWRAP_SINGLES)
        && keyFormat.supportsFeature(SerdeFeature.WRAP_SINGLES)
    ) {
      return Optional.of(SerdeFeature.UNWRAP_SINGLES);
    }
    return Optional.empty();
  }

  private static Optional<SerdeFeature> getValueWrapping(
      final boolean singleColumn,
      final Format valueFormat,
      final SerdeFeatures explicitFeatures,
      final KsqlConfig ksqlConfig
  ) {
    final Optional<SerdeFeature> valueWrapping = explicitFeatures
        .findAny(SerdeFeatures.WRAPPING_FEATURES);

    if (valueWrapping.isPresent()) {
      validateExplicitValueWrapping(singleColumn, valueFormat, valueWrapping.get());
      return valueWrapping;
    }

    return getDefaultValueWrapping(singleColumn, valueFormat, ksqlConfig);
  }

  private static void validateExplicitValueWrapping(
      final boolean singleColumn,
      final Format valueFormat,
      final SerdeFeature wrappingFeature
  ) {
    if (!valueFormat.supportedFeatures().contains(wrappingFeature)) {
      final boolean value = wrappingFeature == SerdeFeature.WRAP_SINGLES;
      throw new KsqlException("Format '" + valueFormat.name() + "' "
          + "does not support '" + CommonCreateConfigs.WRAP_SINGLE_VALUE + "' set to '"
          + value + "'.");
    }

    if (!singleColumn) {
      throw new KsqlException("'" + CommonCreateConfigs.WRAP_SINGLE_VALUE
          + "' is only valid for single-field value schemas");
    }
  }

  private static Optional<SerdeFeature> getDefaultValueWrapping(
      final boolean singleColumn,
      final Format valueFormat,
      final KsqlConfig ksqlConfig
  ) {
    if (!singleColumn) {
      return Optional.empty();
    }

    final Boolean valueWrapping = ksqlConfig.getBoolean(KsqlConfig.KSQL_WRAP_SINGLE_VALUES);
    if (valueWrapping == null) {
      return Optional.empty();
    }

    final SerdeFeature feature = valueWrapping
        ? SerdeFeature.WRAP_SINGLES
        : SerdeFeature.UNWRAP_SINGLES;

    if (!valueFormat.supportsFeature(feature)) {
      return Optional.empty();
    }

    return Optional.of(feature);
  }

  private static boolean formatSupportsMultipleColumns(final KeyFormat format) {
    return !format.getFormat().equals(KafkaFormat.NAME)
        && !format.getFormat().equals(NoneFormat.NAME);
  }
}
