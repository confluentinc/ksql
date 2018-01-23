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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReferentialIntegrityTableTest {

  @Test
  public void shouldAddNewSourceSinkCorrectly() {
    String sourceName = "Test1";
    String sinkName = "Test2";
    String queryId = "Query1";
    ReferentialIntegrityTable referentialIntegrityTable = new ReferentialIntegrityTable();
    referentialIntegrityTable.addSourceNames(Collections.singleton(sourceName), queryId);
    referentialIntegrityTable.addSinkNames(Collections.singleton(sinkName), queryId);
    assertTrue(referentialIntegrityTable.getSourceForQuery(sourceName).contains(queryId));
    assertTrue(referentialIntegrityTable.getSinkForQuery(sinkName).contains(queryId));
  }

  @Test
  public void shouldAddMultipleSourceSinkCorrectlt() {
    ReferentialIntegrityTable referentialIntegrityTable = new ReferentialIntegrityTable();
    referentialIntegrityTable.addSourceNames(new HashSet<>(Arrays.asList("Source1", "Source2")),
                                             "query1");
    referentialIntegrityTable.addSinkNames(new HashSet<>(Arrays.asList("Sink1", "Sink2")),
                                           "query1");
    assertTrue(referentialIntegrityTable.getSourceForQuery("Source1").contains("query1"));
    assertTrue(referentialIntegrityTable.getSourceForQuery("Source2").contains("query1"));
    assertTrue(referentialIntegrityTable.getSinkForQuery("Sink1").contains("query1"));
    assertTrue(referentialIntegrityTable.getSinkForQuery("Sink2").contains("query1"));
  }


  @Test
  public void shouldCheckForSafeDropCorrectly() {
    ReferentialIntegrityTable referentialIntegrityTable = new ReferentialIntegrityTable();
    referentialIntegrityTable.addSourceNames(new HashSet<>(Arrays.asList("Source1", "Source2")),
                                             "query1");
    referentialIntegrityTable.addSinkNames(new HashSet<>(Arrays.asList("Sink1", "Sink2")),
                                           "query1");
    assertTrue(referentialIntegrityTable.isSafeToDrop("Test0"));
    assertFalse(referentialIntegrityTable.isSafeToDrop("Source1"));
  }

}
