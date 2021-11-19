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
package io.confluent.ksql.util;

import static io.confluent.ksql.GenericKey.genericKey;
import static io.confluent.ksql.GenericRow.genericRow;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableListMultimap.Builder;
import com.google.common.collect.Multimap;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeFeatures;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class UserDataProviderBig extends TestDataProvider {
    private static final int NUM_ROWS = 1000;

    private static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
            .keyColumn(ColumnName.of("USERID"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("REGISTERTIME"), SqlTypes.BIGINT)
            .valueColumn(ColumnName.of("GENDER"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("REGIONID"), SqlTypes.STRING)
            .build();

    private static final PhysicalSchema PHYSICAL_SCHEMA = PhysicalSchema
            .from(LOGICAL_SCHEMA, SerdeFeatures.of(), SerdeFeatures.of());

    private static final Builder<GenericKey, GenericRow> BUILDER = ImmutableListMultimap.builder();

    private static final List<String> GENDERS = Arrays.asList("MALE", "FEMALE");
    private static final List<String> REGIONS = Arrays.asList("REGION_1", "REGION_2", "REGION_3", "REGION_4");

    private static final Multimap<GenericKey, GenericRow> ROWS = buildRows();

    public UserDataProviderBig() {
        super("USER", PHYSICAL_SCHEMA, ROWS);
    }

    private static Multimap<GenericKey, GenericRow> buildRows() {
        for (int i = 0; i < NUM_ROWS; i++){
            String gender = GENDERS.get(ThreadLocalRandom.current().nextInt(2));
            String region = REGIONS.get(ThreadLocalRandom.current().nextInt(4));
            BUILDER.put(genericKey("USER_" + i),
                    genericRow((long) i, gender, region));
        }
        return BUILDER.build();
    }
    public int getNumRecords() {
        return NUM_ROWS;
    }
}