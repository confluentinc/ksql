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

package io.confluent.ksql.schema.ksql;

import static io.confluent.ksql.util.SchemaUtil.ROWKEY_NAME;
import static io.confluent.ksql.util.SchemaUtil.ROWTIME_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

import org.junit.Test;

public class IdentifiersTest {

  @Test
  public void shouldMarkRowTimeAsImplicit() {
    assertThat(Identifiers.isImplicitColumnName(ROWTIME_NAME), is(true));
  }

  @Test
  public void shouldMarkRowKeyAsImplicit() {
    assertThat(Identifiers.isImplicitColumnName(ROWKEY_NAME), is(true));
  }

  @Test
  public void shouldMarkAsImplicitRegardlessOfCase() {
    assertThat(Identifiers.isImplicitColumnName(ROWTIME_NAME.toLowerCase()), is(true));
    assertThat(Identifiers.isImplicitColumnName(ROWTIME_NAME.toUpperCase()), is(true));
  }

  @Test
  public void shouldNotMarkOtherFieldsAsImplicit() {
    assertThat(Identifiers.isImplicitColumnName("other"), is(false));
  }

  @Test
  public void shouldHaveRowTimeInImplicits() {
    assertThat(Identifiers.implicitColumnNames(), hasItem(ROWTIME_NAME));
  }

  @Test
  public void shouldHaveRowKeyInImplicits() {
    assertThat(Identifiers.implicitColumnNames(), hasItem(ROWKEY_NAME));
  }
}