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
import io.confluent.ksql.util.BinPackedPersistentQueryMetadataImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentQueryCleanupImpl implements PersistentQueryCleanup {
  private static final Logger LOG = LoggerFactory.getLogger(PersistentQueryCleanupImpl.class);

  private final String stateDir;
  private final ServiceContext serviceContext;
  private final QueryCleanupService queryCleanupService;
  private final KsqlConfig ksqlConfig;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public PersistentQueryCleanupImpl(final String stateDir,
                                    final ServiceContext serviceContext,
                                    final KsqlConfig ksqlConfig) {
    this.stateDir = stateDir;
    this.ksqlConfig = ksqlConfig;
    this.serviceContext = serviceContext;
    queryCleanupService = new QueryCleanupService();
    queryCleanupService.startAsync();
  }

  public void cleanupLeakedQueries(final List<PersistentQueryMetadata> persistentQueries) {
    final Set<String> stateStoreNames =
        persistentQueries
            .stream()
            .flatMap(s -> {
              final List<String> doNotDelete = new ArrayList<>(
                  Collections.singletonList(s.getQueryApplicationId()));
              if (s instanceof BinPackedPersistentQueryMetadataImpl) {
                doNotDelete.add(s.getQueryApplicationId()
                    + "/__"
                    + s.getQueryId().toString()
                    + "__");
              }
              return doNotDelete.stream();
            })
            .collect(Collectors.toSet());

    final String[] stateDirFileNames = new File(stateDir).list();
    if (stateDirFileNames == null) {
      LOG.info("No state stores to clean up");
    } else {
      final Set<String> allStateStores = Arrays.stream(stateDirFileNames)
          .flatMap(f -> {
            final String[] fileNames =  new File(stateDir + "/" + f).list();
            if (null == fileNames) {
              return Stream.of(f);
            } else if (Arrays.stream(fileNames).anyMatch(t -> t.matches("__*__"))) {
              return Arrays.stream(fileNames)
                  .filter(t -> t.matches("__*__"))
                  .map(s -> f + "/" + s);
            } else {
              return Stream.of(f);
            }
          })
          .collect(Collectors.toSet());
      allStateStores.removeAll(stateStoreNames);
      allStateStores.forEach((storeName) -> queryCleanupService.addCleanupTask(
          new QueryCleanupService.QueryCleanupTask(
            serviceContext,
            storeName.split("/")[0],
            1 <  storeName.split("__").length
                ? Optional.of(storeName.split("__")[1])
                : Optional.empty(),
            false,
            stateDir,
            ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG),
            ksqlConfig.getString(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG))));
    }
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public QueryCleanupService getQueryCleanupService() {
    return queryCleanupService;
  }
}
