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
package io.confluent.ksql.serde.connect;

import static io.confluent.ksql.serde.SchemaTranslationPolicy.ORIGINAL_FIELD_NAME;
import static io.confluent.ksql.serde.SchemaTranslationPolicy.UPPERCASE_FIELD_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import io.confluent.ksql.serde.SchemaTranslationPolicy;
import java.util.Set;
import org.junit.Test;

public class SchemaTranslationPolicyTest {

  @Test
  public void shouldReturnIncompatibleSet() {
    // When:
    Set<SchemaTranslationPolicy> uppercaseIncompables = UPPERCASE_FIELD_NAME.getIncompatibleWith();
    Set<SchemaTranslationPolicy> originalIncompables = ORIGINAL_FIELD_NAME.getIncompatibleWith();

    // Then:
    assertThat(uppercaseIncompables, containsInAnyOrder(ORIGINAL_FIELD_NAME));
    assertThat(originalIncompables, containsInAnyOrder(UPPERCASE_FIELD_NAME));
  }

}
