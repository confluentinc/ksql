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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ReferentialIntegrityTable {

  private final Map<String, ReferentialIntegrityTableEntry> referentialIntegrityTable;

  public ReferentialIntegrityTable() {
    referentialIntegrityTable = new HashMap<String, ReferentialIntegrityTableEntry>();
  }

  public void addSourceNames(Set<String> sourceNames, String queryId) {
    for (String sourceName: sourceNames) {
      if (referentialIntegrityTable.containsKey(sourceName)) {
        ReferentialIntegrityTableEntry referentialIntegrityTableEntry =
            referentialIntegrityTable.get(sourceName);
        referentialIntegrityTableEntry.getSourceForQueries().add(queryId);
      } else {
        ReferentialIntegrityTableEntry referentialIntegrityTableEntry = new
            ReferentialIntegrityTableEntry(sourceName);
        referentialIntegrityTableEntry.getSourceForQueries().add(queryId);
        referentialIntegrityTable.put(sourceName, referentialIntegrityTableEntry);
      }
    }
  }

  public void addSinkNames(Set<String> sinkNames, String queryId) {
    for (String sinkName: sinkNames) {
      if (referentialIntegrityTable.containsKey(sinkName)) {
        ReferentialIntegrityTableEntry referentialIntegrityTableEntry =
            referentialIntegrityTable.get(sinkName);
        referentialIntegrityTableEntry.getSinkForQueries().add(queryId);
      } else {
        ReferentialIntegrityTableEntry referentialIntegrityTableEntry = new
            ReferentialIntegrityTableEntry(sinkName);
        referentialIntegrityTableEntry.getSinkForQueries().add(queryId);
        referentialIntegrityTable.put(sinkName, referentialIntegrityTableEntry);
      }
    }
  }

  public void removeQueryFromReferentialIntegrityTable(String queryId) {
    for (ReferentialIntegrityTableEntry referentialIntegrityTableEntry:
        referentialIntegrityTable.values()) {
      referentialIntegrityTableEntry.getSourceForQueries().remove(queryId);
      referentialIntegrityTableEntry.getSinkForQueries().remove(queryId);
    }
  }

  public boolean isSafeToDrop(String sourceName) {
    if (!referentialIntegrityTable.containsKey(sourceName)) {
      return true;
    }
    ReferentialIntegrityTableEntry referentialIntegrityTableEntry =
        referentialIntegrityTable.get(sourceName);
    if (referentialIntegrityTableEntry.getSinkForQueries().isEmpty() &&
        referentialIntegrityTableEntry.getSourceForQueries().isEmpty()) {
      return true;
    }
    return false;
  }

  public Set<String> getSourceForQuery(String sourceName) {
    return referentialIntegrityTable.get(sourceName).getSourceForQueries();
  }

  public Set<String> getSinkForQuery(String sourceName) {
    return referentialIntegrityTable.get(sourceName).getSinkForQueries();
  }

}
