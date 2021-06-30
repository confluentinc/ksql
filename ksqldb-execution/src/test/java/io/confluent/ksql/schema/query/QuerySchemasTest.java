/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.schema.query;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class QuerySchemasTest {
    private static final boolean IS_KEY = true;
    private static final boolean IS_VALUE = false;

    private static final KeyFormat JSON_KEY_FORMAT = KeyFormat.of(
        FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of(), Optional.empty()
    );

    private static final KeyFormat AVRO_KEY_FORMAT = KeyFormat.of(
        FormatInfo.of(FormatFactory.AVRO.name()), SerdeFeatures.of(), Optional.empty()
    );

    private static final KeyFormat NONE_KEY_FORMAT = KeyFormat.of(
        FormatInfo.of(FormatFactory.NONE.name()), SerdeFeatures.of(), Optional.empty()
    );

    private static final ValueFormat JSON_VALUE_FORMAT = ValueFormat.of(
        FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of()
    );

    private static final ValueFormat AVRO_VALUE_FORMAT = ValueFormat.of(
        FormatInfo.of(FormatFactory.AVRO.name()), SerdeFeatures.of()
    );

    private static final ValueFormat NONE_VALUE_FORMAT = ValueFormat.of(
        FormatInfo.of(FormatFactory.NONE.name()), SerdeFeatures.of()
    );

    private static final LogicalSchema LOGICAL_SCHEMA_X = LogicalSchema.builder().build();

    private QuerySchemas querySchemas;

    @Before
    public void setup() {
        querySchemas = new QuerySchemas();
    }

    @Test
    public void shouldTrackSerdesCreation() {
        // When
        querySchemas.trackKeySerdeCreation("K_LOGGER", LOGICAL_SCHEMA_X, JSON_KEY_FORMAT);
        querySchemas.trackValueSerdeCreation("V_LOGGER", LOGICAL_SCHEMA_X, JSON_VALUE_FORMAT);

        // Then
        final Map<String, QuerySchemas.SchemaInfo> loggerSchemaInfo =
            querySchemas.getLoggerSchemaInfo();
        assertThat(loggerSchemaInfo.keySet(), is(ImmutableSet.of("K_LOGGER", "V_LOGGER")));
        assertThat(loggerSchemaInfo.get("K_LOGGER"), is(new QuerySchemas.SchemaInfo(
            LOGICAL_SCHEMA_X, Optional.of(JSON_KEY_FORMAT), Optional.empty())));
        assertThat(loggerSchemaInfo.get("V_LOGGER"), is(new QuerySchemas.SchemaInfo(
            LOGICAL_SCHEMA_X, Optional.empty(), Optional.of(JSON_VALUE_FORMAT))));
    }

    @Test
    public void shouldMergeKeyAndValueSerdeCreation() {
        // When
        querySchemas.trackKeySerdeCreation("LOGGER_1", LOGICAL_SCHEMA_X, JSON_KEY_FORMAT);
        querySchemas.trackValueSerdeCreation("LOGGER_1", LOGICAL_SCHEMA_X, JSON_VALUE_FORMAT);

        // Then
        final Map<String, QuerySchemas.SchemaInfo> loggerSchemaInfo =
            querySchemas.getLoggerSchemaInfo();
        assertThat(loggerSchemaInfo.keySet(), is(ImmutableSet.of("LOGGER_1")));
        assertThat(loggerSchemaInfo.get("LOGGER_1"), is(new QuerySchemas.SchemaInfo(
            LOGICAL_SCHEMA_X, Optional.of(JSON_KEY_FORMAT), Optional.of(JSON_VALUE_FORMAT))));
    }

    @Test
    public void shouldTrackSerdeOps() {
        // When
        querySchemas.trackSerdeOp("topic1", IS_KEY, "K_LOGGER");
        querySchemas.trackSerdeOp("topic1", IS_VALUE, "V_LOGGER");

        // Then
        final Map<String, Map<Boolean, Set<String>>> loggers = querySchemas.getTopicsToLoggers();
        assertThat(loggers.keySet(), is(ImmutableSet.of("topic1")));
        assertThat(loggers.get("topic1"), is(ImmutableMap.of(
            IS_KEY, ImmutableSet.of("K_LOGGER"),
            IS_VALUE, ImmutableSet.of("V_LOGGER")
        )));
    }

    @Test
    public void shouldTrackSerdeOpsWithMultipleKeysOnSameTopic() {
        // When
        querySchemas.trackSerdeOp("topic1", IS_KEY, "K_LOGGER_1");
        querySchemas.trackSerdeOp("topic1", IS_KEY, "K_LOGGER_2");

        // Then
        final Map<String, Map<Boolean, Set<String>>> loggers = querySchemas.getTopicsToLoggers();
        assertThat(loggers.keySet(), is(ImmutableSet.of("topic1")));
        assertThat(loggers.get("topic1"), is(ImmutableMap.of(
            IS_KEY, ImmutableSet.of("K_LOGGER_1", "K_LOGGER_2")
        )));
    }

    @Test
    public void shouldTrackSerdeOpsWithMultipleValuesOnSameTopic() {
        // When
        querySchemas.trackSerdeOp("topic1", IS_VALUE, "V_LOGGER_1");
        querySchemas.trackSerdeOp("topic1", IS_VALUE, "V_LOGGER_2");

        // Then
        final Map<String, Map<Boolean, Set<String>>> loggers = querySchemas.getTopicsToLoggers();
        assertThat(loggers.keySet(), is(ImmutableSet.of("topic1")));
        assertThat(loggers.get("topic1"), is(ImmutableMap.of(
            IS_VALUE, ImmutableSet.of("V_LOGGER_1", "V_LOGGER_2")
        )));
    }

    @Test
    public void shouldGetKeyAndValueSchemaWithOneLoggerWriting() {
        // Given
        querySchemas.trackKeySerdeCreation("K_LOGGER", LOGICAL_SCHEMA_X, JSON_KEY_FORMAT);
        querySchemas.trackValueSerdeCreation("V_LOGGER", LOGICAL_SCHEMA_X, JSON_VALUE_FORMAT);
        querySchemas.trackSerdeOp("topic1", IS_KEY, "K_LOGGER");
        querySchemas.trackSerdeOp("topic1", IS_VALUE, "V_LOGGER");

        // When
        final QuerySchemas.MultiSchemaInfo info = querySchemas.getTopicInfo("topic1");

        // Then
        assertThat(info.getKeySchemas().size(), is(1));
        assertThat(info.getValueSchemas().size(), is(1));
        assertThat(Iterables.getOnlyElement(info.getKeySchemas()), is(
            new QuerySchemas.SchemaInfo(LOGICAL_SCHEMA_X,
                Optional.of(JSON_KEY_FORMAT),
                Optional.empty())));
        assertThat(Iterables.getOnlyElement(info.getValueSchemas()), is(
            new QuerySchemas.SchemaInfo(LOGICAL_SCHEMA_X,
                Optional.empty(),
                Optional.of(JSON_VALUE_FORMAT))));
        assertThat(Iterables.getOnlyElement(info.getKeyFormats()), is(JSON_KEY_FORMAT));
        assertThat(Iterables.getOnlyElement(info.getValueFormats()), is(JSON_VALUE_FORMAT));
    }

    @Test
    public void shouldGetCombinedKeyAndValueSchemaWithMultipleLoggersWriting() {
        // Given
        querySchemas.trackKeySerdeCreation("K_LOGGER_1", LOGICAL_SCHEMA_X, JSON_KEY_FORMAT);
        querySchemas.trackKeySerdeCreation("K_LOGGER_2", LOGICAL_SCHEMA_X, JSON_KEY_FORMAT);
        querySchemas.trackValueSerdeCreation("V_LOGGER_1", LOGICAL_SCHEMA_X, JSON_VALUE_FORMAT);
        querySchemas.trackValueSerdeCreation("V_LOGGER_2", LOGICAL_SCHEMA_X, JSON_VALUE_FORMAT);
        querySchemas.trackSerdeOp("topic1", IS_KEY, "K_LOGGER_1");
        querySchemas.trackSerdeOp("topic1", IS_KEY, "K_LOGGER_2");
        querySchemas.trackSerdeOp("topic1", IS_VALUE, "V_LOGGER_1");
        querySchemas.trackSerdeOp("topic1", IS_VALUE, "V_LOGGER_2");

        // When
        final QuerySchemas.MultiSchemaInfo info = querySchemas.getTopicInfo("topic1");

        // Then
        assertThat(info.getKeySchemas().size(), is(1));
        assertThat(info.getValueSchemas().size(), is(1));
        assertThat(Iterables.getOnlyElement(info.getKeySchemas()), is(
            new QuerySchemas.SchemaInfo(LOGICAL_SCHEMA_X,
                Optional.of(JSON_KEY_FORMAT),
                Optional.empty())));
        assertThat(Iterables.getOnlyElement(info.getValueSchemas()), is(
            new QuerySchemas.SchemaInfo(LOGICAL_SCHEMA_X,
                Optional.empty(),
                Optional.of(JSON_VALUE_FORMAT))));
        assertThat(Iterables.getOnlyElement(info.getKeyFormats()), is(JSON_KEY_FORMAT));
        assertThat(Iterables.getOnlyElement(info.getValueFormats()), is(JSON_VALUE_FORMAT));
    }

    @Test
    public void shouldGetMultiKeyAndValueSchemaWithMultipleLoggersWritingDifferentFormats() {
        // Given
        querySchemas.trackKeySerdeCreation("K_LOGGER_1", LOGICAL_SCHEMA_X, JSON_KEY_FORMAT);
        querySchemas.trackKeySerdeCreation("K_LOGGER_2", LOGICAL_SCHEMA_X, AVRO_KEY_FORMAT);
        querySchemas.trackValueSerdeCreation("V_LOGGER_1", LOGICAL_SCHEMA_X, JSON_VALUE_FORMAT);
        querySchemas.trackValueSerdeCreation("V_LOGGER_2", LOGICAL_SCHEMA_X, AVRO_VALUE_FORMAT);
        querySchemas.trackSerdeOp("topic1", IS_KEY, "K_LOGGER_1");
        querySchemas.trackSerdeOp("topic1", IS_KEY, "K_LOGGER_2");
        querySchemas.trackSerdeOp("topic1", IS_VALUE, "V_LOGGER_1");
        querySchemas.trackSerdeOp("topic1", IS_VALUE, "V_LOGGER_2");

        // When
        final QuerySchemas.MultiSchemaInfo info = querySchemas.getTopicInfo("topic1");

        // Then
        assertThat(info.getKeySchemas().size(), is(2));
        assertThat(info.getValueSchemas().size(), is(2));
        assertThat(info.getKeySchemas(), is(
            ImmutableSet.of(
                new QuerySchemas.SchemaInfo(LOGICAL_SCHEMA_X,
                    Optional.of(JSON_KEY_FORMAT),
                    Optional.empty()),
                new QuerySchemas.SchemaInfo(LOGICAL_SCHEMA_X,
                    Optional.of(AVRO_KEY_FORMAT),
                    Optional.empty()))));
        assertThat(info.getValueSchemas(), is(
            ImmutableSet.of(
                new QuerySchemas.SchemaInfo(LOGICAL_SCHEMA_X,
                    Optional.empty(),
                    Optional.of(JSON_VALUE_FORMAT)),
                new QuerySchemas.SchemaInfo(LOGICAL_SCHEMA_X,
                    Optional.empty(),
                    Optional.of(AVRO_VALUE_FORMAT)))));
        assertThat(info.getKeyFormats(),
            is(ImmutableSet.of(AVRO_KEY_FORMAT, JSON_KEY_FORMAT)));
        assertThat(info.getValueFormats(),
            is(ImmutableSet.of(AVRO_VALUE_FORMAT, JSON_VALUE_FORMAT)));
    }

    @Test
    public void shouldGetEmptyValueSchemaIfNoValueWasDetected() {
        // Given
        querySchemas.trackSerdeOp("topic1", IS_KEY, "K_LOGGER_1");
        querySchemas.trackKeySerdeCreation("K_LOGGER_1", LOGICAL_SCHEMA_X, JSON_KEY_FORMAT);

        // When
        final QuerySchemas.MultiSchemaInfo info = querySchemas.getTopicInfo("topic1");

        // Then
        assertThat(info.getKeySchemas().size(), is(1));
        assertThat(info.getValueSchemas().size(), is(0));
        assertThat(Iterables.getOnlyElement(info.getKeySchemas()), is(
            new QuerySchemas.SchemaInfo(LOGICAL_SCHEMA_X,
                Optional.of(JSON_KEY_FORMAT),
                Optional.empty())));
        assertThat(info.getValueFormats().size(), is(0));
        assertThat(Iterables.getOnlyElement(info.getKeyFormats()), is(JSON_KEY_FORMAT));
    }

    @Test
    public void shouldGetEmptyKeySchemaIfNoKeyWasDetected() {
        // Given
        querySchemas.trackSerdeOp("topic1", IS_VALUE, "V_LOGGER_1");
        querySchemas.trackValueSerdeCreation("V_LOGGER_1", LOGICAL_SCHEMA_X, JSON_VALUE_FORMAT);

        // When
        final QuerySchemas.MultiSchemaInfo info = querySchemas.getTopicInfo("topic1");

        // Then
        assertThat(info.getKeySchemas().size(), is(0));
        assertThat(info.getValueSchemas().size(), is(1));
        assertThat(Iterables.getOnlyElement(info.getValueSchemas()), is(
            new QuerySchemas.SchemaInfo(LOGICAL_SCHEMA_X,
                Optional.empty(),
                Optional.of(JSON_VALUE_FORMAT))));
        assertThat(info.getKeyFormats().size(), is(0));
        assertThat(Iterables.getOnlyElement(info.getValueFormats()), is(JSON_VALUE_FORMAT));
    }

    @Test
    public void shouldThrowIfTopicNameIsNotFound() {
        // When
        final Exception e = assertThrows(
            IllegalArgumentException.class,
            () -> querySchemas.getTopicInfo("t1"));

        // Then
        assertThat(e.getMessage(), is("Unknown topic: t1"));
    }
}
