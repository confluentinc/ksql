/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the
 * License.
 */

package io.confluent.ksql.function.udf;

import static io.confluent.ksql.function.FunctionCategory.MAP;
import static io.confluent.ksql.function.FunctionCategory.OTHER;

import com.google.common.testing.EqualsTester;
import org.junit.Test;

public class UdfMetadataTest {

  @Test
  public void shouldImplementHashCodeAndEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(
            new UdfMetadata("name", "desc", "auth", "ver", OTHER, "path"),
            new UdfMetadata("name", "desc", "auth", "ver", OTHER, "path"))
        .addEqualityGroup(new UdfMetadata("DIF", "desc", "auth", "ver", OTHER, "path"))
        .addEqualityGroup(new UdfMetadata("name", "DIF", "auth", "ver", OTHER, "path"))
        .addEqualityGroup(new UdfMetadata("name", "desc", "DIF", "ver", OTHER, "path"))
        .addEqualityGroup(new UdfMetadata("name", "desc", "auth", "DIF", OTHER, "path"))
        .addEqualityGroup(new UdfMetadata("name", "desc", "auth", "ver", MAP, "path"))
        .addEqualityGroup(new UdfMetadata("name", "desc", "auth", "ver", OTHER, "DIF"))
        .testEquals();
  }
}
