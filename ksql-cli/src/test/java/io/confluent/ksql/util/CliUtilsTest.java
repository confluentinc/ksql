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

package io.confluent.ksql.util;

import org.junit.Test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.ConnectException;
import java.sql.SQLDataException;

/**
 * Unit tests for class {@link CliUtils}.
 *
 * @see CliUtils
 */
public class CliUtilsTest {

  @Test
  public void testGetAvroSchemaThrowsKsqlException() {
    CliUtils cliUtils = new CliUtils();

    try {
      cliUtils.getAvroSchema("TZGUM?ploV");
      fail("Expecting exception: KsqlException");
    } catch (KsqlException e) {
      assertEquals(CliUtils.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }
}