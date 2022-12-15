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

package io.confluent.ksql.rest.util;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.engine.QueryCleanupService;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentQueryCleanupImpl implements PersistentQueryCleanup {
  private static final Logger LOG = LoggerFactory.getLogger(PersistentQueryCleanupImpl.class);

  private final String stateDir;
  private final ServiceContext serviceContext;
  private final QueryCleanupService queryCleanupService;

  public PersistentQueryCleanupImpl(final String stateDir, final ServiceContext serviceContext) {
    this.stateDir = stateDir;
    this.serviceContext = serviceContext;
    queryCleanupService = new QueryCleanupService();
    queryCleanupService.startAsync();
  }

  public void cleanupLeakedQueries(final List<PersistentQueryMetadata> persistentQueries) {
    final Set<String> stateStoreNames =
        persistentQueries
        .stream()
        .map(PersistentQueryMetadata::getQueryApplicationId)
        .collect(Collectors.toSet());

    final String[] stateDirFileNames = new File(stateDir).list();
    if (stateDirFileNames == null) {
      LOG.info("No state stores to clean up");
    } else {
      final Set<String> allStateStores = new HashSet<>(Arrays.asList(stateDirFileNames));
      allStateStores.removeAll(stateStoreNames);
      allStateStores.forEach((appId) -> queryCleanupService.addCleanupTask(
          new QueryCleanupService.QueryCleanupTask(
          serviceContext,
          appId,
          false,
          stateDir)));
    }
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public QueryCleanupService getQueryCleanupService() {
    return queryCleanupService;
  }
}
