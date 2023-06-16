/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.metastore;

import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import java.util.Set;

public interface MutableMetaStore extends MetaStore {

  void putSource(DataSource dataSource, boolean allowReplace);

  default void deleteSource(SourceName sourceName) {
    deleteSource(sourceName, false);
  }

  void deleteSource(SourceName sourceName, boolean restoreInProgress);

  void addSourceReferences(SourceName sourceName, Set<SourceName> sourceReferences);

  MutableMetaStore copy();
}
