/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.connect;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import org.junit.Test;

public class ConnectorTest {

  @Test
  public void shouldImplementHashAndEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new Connector("foo", foo -> true, foo -> foo, DataSourceType.KTABLE, "key"),
            new Connector("foo", foo -> false, foo -> foo, DataSourceType.KTABLE, "key"),
            new Connector("foo", foo -> false, foo -> foo, DataSourceType.KTABLE, "key")
        ).addEqualityGroup(
            new Connector("bar", foo -> true, foo -> foo, DataSourceType.KTABLE, "key")
        ).addEqualityGroup(
            new Connector("foo", foo -> true, foo -> foo, DataSourceType.KTABLE, "key2")
        ).addEqualityGroup(
            new Connector("foo", foo -> true, foo -> foo, DataSourceType.KSTREAM, "key")
        )
        .testEquals();
  }

}