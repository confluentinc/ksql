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

package io.confluent.ksql.execution.streams.timestamp;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.name.ColumnName;
import org.junit.Test;

public class StringTimestampExtractionPolicyTest {
  @Test
  public void shouldTestEqualityCorrectly() {
    new EqualsTester()
        .addEqualityGroup(
            new StringTimestampExtractionPolicy(ColumnName.of("field1"), "yyMMddHHmmssZ"),
            new StringTimestampExtractionPolicy(ColumnName.of("field1"), "yyMMddHHmmssZ"))
        .addEqualityGroup(new StringTimestampExtractionPolicy(ColumnName.of("field2"), "yyMMddHHmmssZ"))
        .addEqualityGroup(new StringTimestampExtractionPolicy(ColumnName.of("field1"), "ddMMyyHHmmssZ"))
        .testEquals();
  }
}
