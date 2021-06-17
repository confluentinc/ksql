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

package io.confluent.ksql.schema.query;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
    public void shouldReturnFormatsInfoWithOneKeyAndValueFormat() {
        // Given
        querySchemas.trackKeySerdeCreation("K_LOGGER", LOGICAL_SCHEMA_X, JSON_KEY_FORMAT);
        querySchemas.trackValueSerdeCreation("V_LOGGER", LOGICAL_SCHEMA_X, JSON_VALUE_FORMAT);
        querySchemas.trackSerdeOp("topic1", IS_KEY, "K_LOGGER");
        querySchemas.trackSerdeOp("topic1", IS_VALUE, "V_LOGGER");

        // When
        final QuerySchemas.TopicFormatsInfo info = querySchemas.getTopicFormatsInfo("topic1");

        // Then
        assertThat(info.keyFormat(), is(JSON_KEY_FORMAT));
        assertThat(info.valueFormat(), is(JSON_VALUE_FORMAT));
    }

    @Test
    public void shouldReturnFormatsInfoWithOneSingleKVFormatWhenMultipleLoggersWithSameKVFormatsAreFound() {
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
        final QuerySchemas.TopicFormatsInfo info = querySchemas.getTopicFormatsInfo("topic1");

        // Then
        assertThat(info.keyFormat(), is(JSON_KEY_FORMAT));
        assertThat(info.valueFormat(), is(JSON_VALUE_FORMAT));
    }

    @Test
    public void shouldReturnFormatsInfoWithEmptyValueFormatIsNoValueSchemaIsFound() {
        // Given
        querySchemas.trackSerdeOp("topic1", IS_KEY, "K_LOGGER_1");
        querySchemas.trackKeySerdeCreation("K_LOGGER_1", LOGICAL_SCHEMA_X, JSON_KEY_FORMAT);

        // When
        final QuerySchemas.TopicFormatsInfo info = querySchemas.getTopicFormatsInfo("topic1");

        // Then
        assertThat(info.keyFormat(), is(JSON_KEY_FORMAT));
        assertThat(info.valueFormat(), is(NONE_VALUE_FORMAT));
    }

    @Test
    public void shouldThrowFormatsInfoIfNoKeySchemaIsFound() {
        // Given
        querySchemas.trackSerdeOp("topic1", IS_VALUE, "V_LOGGER_1");
        querySchemas.trackValueSerdeCreation("V_LOGGER_1", LOGICAL_SCHEMA_X, JSON_VALUE_FORMAT);

        // When
        final Exception e = assertThrows(
            IllegalStateException.class,
            () -> querySchemas.getTopicFormatsInfo("topic1"));

        // Then
        assertThat(e.getMessage(),
            containsString("Zero key formats registered for topic."));
        assertThat(e.getMessage(),
            containsString("topic: topic1"));
        assertThat(e.getMessage(),
            containsString("formats: []"));
        assertThat(e.getMessage(),
            containsString("loggers: []"));
    }

    @Test
    public void shouldThrowIfTopicNameIsNotFound() {
        // When
        final Exception e = assertThrows(
            IllegalArgumentException.class,
            () -> querySchemas.getTopicFormatsInfo("t1"));

        // Then
        assertThat(e.getMessage(), is("Unknown topic: t1"));
    }

    @Test
    public void shouldThrowIfMultipleKeyLoggersSchemasAreFound() {
        // Given
        querySchemas.trackKeySerdeCreation("K_LOGGER_1", LOGICAL_SCHEMA_X, JSON_KEY_FORMAT);
        querySchemas.trackKeySerdeCreation("K_LOGGER_2", LOGICAL_SCHEMA_X, AVRO_KEY_FORMAT);
        querySchemas.trackSerdeOp("topic1", IS_KEY, "K_LOGGER_1");
        querySchemas.trackSerdeOp("topic1", IS_KEY, "K_LOGGER_2");

        // When
        final Exception e = assertThrows(
            IllegalStateException.class,
            () -> querySchemas.getTopicFormatsInfo("topic1"));

        // Then
        assertThat(e.getMessage(),
            containsString("Multiple key formats registered for topic."));
        assertThat(e.getMessage(),
            containsString("topic: topic1"));
        assertThat(e.getMessage(),
            containsString("formats: [AVRO, JSON]"));
        assertThat(e.getMessage(),
            containsString("loggers: [[K_LOGGER_1], [K_LOGGER_2]]"));
    }

    @Test
    public void shouldThrowIfMultipleValueLoggersSchemasAreFound() {
        // Given
        querySchemas.trackKeySerdeCreation("K_LOGGER_1", LOGICAL_SCHEMA_X, JSON_KEY_FORMAT);
        querySchemas.trackValueSerdeCreation("V_LOGGER_1", LOGICAL_SCHEMA_X, JSON_VALUE_FORMAT);
        querySchemas.trackValueSerdeCreation("V_LOGGER_2", LOGICAL_SCHEMA_X, AVRO_VALUE_FORMAT);
        querySchemas.trackSerdeOp("topic1", IS_KEY, "K_LOGGER_1");
        querySchemas.trackSerdeOp("topic1", IS_VALUE, "V_LOGGER_1");
        querySchemas.trackSerdeOp("topic1", IS_VALUE, "V_LOGGER_2");

        // When
        final Exception e = assertThrows(
            IllegalStateException.class,
            () -> querySchemas.getTopicFormatsInfo("topic1"));

        // Then
        assertThat(e.getMessage(),
            containsString("Multiple value formats registered for topic."));
        assertThat(e.getMessage(),
            containsString("topic: topic1"));
        assertThat(e.getMessage(),
            containsString("formats: [AVRO, JSON]"));
        assertThat(e.getMessage(),
            containsString("loggers: [[V_LOGGER_1], [V_LOGGER_2]"));
    }

    @Test
    public void shouldThrowIfKeySchemaIsNotFound() {
        // Given
        querySchemas.trackSerdeOp("topic1", IS_KEY, "K_LOGGER_1");
        querySchemas.trackSerdeOp("topic1", IS_VALUE, "V_LOGGER_1");
        querySchemas.trackValueSerdeCreation("V_LOGGER_1", LOGICAL_SCHEMA_X, JSON_VALUE_FORMAT);

        // When
        final Exception e = assertThrows(
            IllegalStateException.class,
            () -> querySchemas.getTopicFormatsInfo("topic1"));

        // Then
        assertThat(e.getMessage(),
            containsString("Zero key formats registered for topic."));
        assertThat(e.getMessage(),
            containsString("topic: topic1"));
        assertThat(e.getMessage(),
            containsString("formats: []"));
        assertThat(e.getMessage(),
            containsString("loggers: [[K_LOGGER_1]]"));
    }
}
