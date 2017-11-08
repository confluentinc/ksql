/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.metastore;

import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.KsqlException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Unit tests for class {@link StructuredDataSource}.
 *
 * @see StructuredDataSource
 */
public class StructuredDataSourceTest {

  @Test
  public void testShouldGetStreamDataSourceType() {

    assertEquals(DataSource.DataSourceType.KSTREAM, StructuredDataSource.getDataSourceType("STREAM"));
  }

  @Test
  public void testShouldGetTableDataSourceType() {

    assertEquals(DataSource.DataSourceType.KTABLE, StructuredDataSource.getDataSourceType("TABLE"));
  }

  @Test
  public void testShouldFailOnUnknownDataSourceType() {

    try {
      StructuredDataSource.getDataSourceType("");
      fail("Expecting exception: KsqlException");
    } catch (KsqlException e) {
      assertEquals(StructuredDataSource.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testShouldFailOnNullDataSourceType() {

    try {
      StructuredDataSource.getDataSourceType(null);
      fail("Expecting exception: NullPointerException");
    } catch (NullPointerException e) {
      assertEquals(StructuredDataSource.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }
}