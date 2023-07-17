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

package io.confluent.ksql.rest.entity;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.model.WindowType;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SourceDescriptionTest {

    private static final String SOME_STRING = "some string";
    private static final int SOME_INT = 3;
    private static final boolean SOME_BOOL = true;

    @Mock
    private RunningQuery query1;
    @Mock
    private RunningQuery query2;
    @Mock
    private FieldInfo fieldInfo;
    @Mock
    private QueryTopicOffsetSummary summary;

    @SuppressWarnings("UnstableApiUsage")
    @Test
    public void shouldImplementHashCodeAndEqualsProperty() {
        final List<RunningQuery> readQueries = Collections.singletonList(query1);
        final List<RunningQuery> writeQueries = Collections.singletonList(query2);
        final List<FieldInfo> fields = Collections.singletonList(fieldInfo);
        final List<QueryOffsetSummary> summaries = Collections.singletonList(
            new QueryOffsetSummary("g1", Collections.singletonList(summary)));

        new EqualsTester()
            .addEqualityGroup(
                new SourceDescription(
                    SOME_STRING, Optional.empty(), readQueries, writeQueries, fields, SOME_STRING,
                    SOME_STRING, SOME_STRING, SOME_STRING,
                    SOME_BOOL, SOME_STRING, SOME_STRING, SOME_STRING, SOME_INT, SOME_INT,
                    SOME_STRING, summaries),
                new SourceDescription(
                    SOME_STRING, Optional.empty(), readQueries, writeQueries, fields, SOME_STRING,
                    SOME_STRING, SOME_STRING, SOME_STRING,
                    SOME_BOOL, SOME_STRING, SOME_STRING, SOME_STRING, SOME_INT, SOME_INT,
                    SOME_STRING, summaries)
            )
            .addEqualityGroup(
                new SourceDescription(
                    "diff", Optional.of(WindowType.SESSION), readQueries, writeQueries, fields,
                    SOME_STRING, SOME_STRING, SOME_STRING, SOME_STRING,
                    SOME_BOOL, SOME_STRING, SOME_STRING, SOME_STRING, SOME_INT, SOME_INT,
                    SOME_STRING, summaries)
            )
            .addEqualityGroup(
                new SourceDescription(
                    SOME_STRING, Optional.empty(), ImmutableList.of(), writeQueries, fields,
                    SOME_STRING, SOME_STRING, SOME_STRING, SOME_STRING,
                    SOME_BOOL, SOME_STRING, SOME_STRING, SOME_STRING, SOME_INT, SOME_INT,
                    SOME_STRING, summaries)
            )
            .addEqualityGroup(
                new SourceDescription(
                    SOME_STRING, Optional.empty(), readQueries, ImmutableList.of(), fields,
                    SOME_STRING, SOME_STRING, SOME_STRING, SOME_STRING,
                    SOME_BOOL, SOME_STRING, SOME_STRING, SOME_STRING, SOME_INT, SOME_INT,
                    SOME_STRING, summaries)
            )
            .addEqualityGroup(
                new SourceDescription(
                    SOME_STRING, Optional.empty(), readQueries, writeQueries, ImmutableList.of(),
                    SOME_STRING, SOME_STRING, SOME_STRING, SOME_STRING,
                    SOME_BOOL, SOME_STRING, SOME_STRING, SOME_STRING, SOME_INT, SOME_INT,
                    SOME_STRING, summaries)
            )
            .addEqualityGroup(
                new SourceDescription(
                    SOME_STRING, Optional.empty(), readQueries, writeQueries, fields,
                    SOME_STRING, SOME_STRING, SOME_STRING, SOME_STRING,
                    SOME_BOOL, SOME_STRING, SOME_STRING, SOME_STRING, SOME_INT, SOME_INT,
                    SOME_STRING, ImmutableList.of())
            )
            .addEqualityGroup(
                new SourceDescription(
                    SOME_STRING, Optional.empty(), readQueries, writeQueries, fields, "diff",
                    SOME_STRING, SOME_STRING, SOME_STRING,
                    SOME_BOOL, SOME_STRING, SOME_STRING, SOME_STRING, SOME_INT, SOME_INT,
                    SOME_STRING, summaries)
            )
            .addEqualityGroup(
                new SourceDescription(
                    SOME_STRING, Optional.empty(), readQueries, writeQueries, fields, SOME_STRING,
                     "diff", SOME_STRING, SOME_STRING,
                    SOME_BOOL, SOME_STRING, SOME_STRING, SOME_STRING, SOME_INT, SOME_INT,
                    SOME_STRING, summaries)
            )
            .addEqualityGroup(
                new SourceDescription(
                    SOME_STRING, Optional.empty(), readQueries, writeQueries, fields, SOME_STRING,
                    SOME_STRING, "diff", SOME_STRING,
                    SOME_BOOL, SOME_STRING, SOME_STRING, SOME_STRING, SOME_INT, SOME_INT,
                    SOME_STRING, summaries)
            )
            .addEqualityGroup(
                new SourceDescription(
                    SOME_STRING, Optional.empty(), readQueries, writeQueries, fields, SOME_STRING,
                    SOME_STRING, SOME_STRING, "diff",
                    SOME_BOOL, SOME_STRING, SOME_STRING, SOME_STRING, SOME_INT, SOME_INT,
                    SOME_STRING, summaries)
            )
            .addEqualityGroup(
                new SourceDescription(
                    SOME_STRING, Optional.empty(), readQueries, writeQueries, fields, SOME_STRING,
                    SOME_STRING, SOME_STRING, SOME_STRING,
                    SOME_BOOL, "diff", SOME_STRING, SOME_STRING, SOME_INT, SOME_INT,
                    SOME_STRING, summaries)
            )
            .addEqualityGroup(
                new SourceDescription(
                    SOME_STRING, Optional.empty(), readQueries, writeQueries, fields, SOME_STRING,
                    SOME_STRING, SOME_STRING, SOME_STRING,
                    !SOME_BOOL, SOME_STRING, SOME_STRING, SOME_STRING, SOME_INT, SOME_INT,
                    SOME_STRING, summaries)
            )
            .addEqualityGroup(
                new SourceDescription(
                    SOME_STRING, Optional.empty(), readQueries, writeQueries, fields, SOME_STRING,
                    SOME_STRING, SOME_STRING, SOME_STRING,
                    SOME_BOOL, SOME_STRING, "diff", SOME_STRING, SOME_INT, SOME_INT,
                    SOME_STRING, summaries)
            )
            .addEqualityGroup(
                new SourceDescription(
                    SOME_STRING, Optional.empty(), readQueries, writeQueries, fields, SOME_STRING,
                    SOME_STRING, SOME_STRING, SOME_STRING,
                    SOME_BOOL, SOME_STRING, SOME_STRING, "diff", SOME_INT, SOME_INT,
                    SOME_STRING, summaries)
            )
            .addEqualityGroup(
                new SourceDescription(
                    SOME_STRING, Optional.empty(), readQueries, writeQueries, fields, SOME_STRING,
                    SOME_STRING, SOME_STRING, SOME_STRING,
                    SOME_BOOL, SOME_STRING, SOME_STRING, SOME_STRING, SOME_INT + 1, SOME_INT,
                    SOME_STRING, summaries)
            )
            .addEqualityGroup(
                new SourceDescription(
                    SOME_STRING, Optional.empty(), readQueries, writeQueries, fields, SOME_STRING,
                    SOME_STRING, SOME_STRING, SOME_STRING,
                    SOME_BOOL, SOME_STRING, SOME_STRING, SOME_STRING, SOME_INT, SOME_INT + 1,
                    SOME_STRING, summaries)
            )
            .addEqualityGroup(
                new SourceDescription(
                    SOME_STRING, Optional.empty(), readQueries, writeQueries, fields, SOME_STRING,
                    SOME_STRING, SOME_STRING, SOME_STRING,
                    SOME_BOOL, SOME_STRING, SOME_STRING, SOME_STRING, SOME_INT, SOME_INT,
                    "diff", summaries)
            )
            .addEqualityGroup(
                new SourceDescription(
                    SOME_STRING, Optional.empty(), readQueries, writeQueries, fields, SOME_STRING,
                    SOME_STRING, SOME_STRING, SOME_STRING,
                    SOME_BOOL, SOME_STRING, SOME_STRING, SOME_STRING, SOME_INT, SOME_INT,
                    SOME_STRING,
                    ImmutableList.of(new QueryOffsetSummary("g1",
                        ImmutableList.of(
                            new QueryTopicOffsetSummary(
                                "t1",
                                ImmutableList.of(
                                    new ConsumerPartitionOffsets(0, 1L, 100L, 99L)
                                )
                            )
                        )))))
            .testEquals();
    }
  }
