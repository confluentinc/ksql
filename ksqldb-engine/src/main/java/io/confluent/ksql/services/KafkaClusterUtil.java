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

package io.confluent.ksql.services;

import com.google.common.collect.Iterables;
import io.confluent.ksql.links.DocumentationLinks;
import io.confluent.ksql.util.ExecutorUtil;
import io.confluent.ksql.util.KsqlServerException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class KafkaClusterUtil {
  private static final Logger LOG = LogManager.getLogger(KafkaClusterUtil.class);

  private static final long DESCRIBE_CLUSTER_TIMEOUT_SECONDS = 30;

  private KafkaClusterUtil() {

  }

  public static boolean isAuthorizedOperationsSupported(final Admin adminClient) {
    try {
      final DescribeClusterResult authorizedOperations = adminClient.describeCluster(
          new DescribeClusterOptions().includeAuthorizedOperations(true)
      );

      return authorizedOperations.authorizedOperations().get() != null;
    } catch (final Exception e) {
      if (ExceptionUtils.indexOfType(e, UnsupportedVersionException.class) != -1) {
        LOG.info("Received nested unsupported version error testing authorized operations api", e);
        return false;
      }
      throw new KsqlServerException("Could not get Kafka authorized operations!", e);
    }
  }

  public static Config getConfig(final Admin adminClient) {
    try {
      final Collection<Node> brokers = adminClient.describeCluster().nodes().get();
      final Node broker = Iterables.getFirst(brokers, null);
      if (broker == null) {
        LOG.warn("No available broker found to fetch config info.");
        throw new KsqlServerException(
            "AdminClient discovered an empty Kafka Cluster. "
                + "Check that Kafka is deployed and KSQL is properly configured.");
      }

      final ConfigResource configResource = new ConfigResource(
          ConfigResource.Type.BROKER,
          broker.idString()
      );

      final Map<ConfigResource, Config> brokerConfig = ExecutorUtil.executeWithRetries(
          () -> adminClient.describeConfigs(Collections.singleton(configResource)).all().get(),
          ExecutorUtil.RetryBehaviour.ON_RETRYABLE);

      return brokerConfig.get(configResource);
    } catch (final KsqlServerException e) {
      throw e;
    } catch (final ClusterAuthorizationException e) {
      throw new KsqlServerException("Could not get Kafka cluster configuration. "
          + "Please ensure the ksql principal has " + AclOperation.DESCRIBE_CONFIGS + " rights "
          + "on the Kafka cluster."
          + System.lineSeparator()
          + "See " + DocumentationLinks.SECURITY_REQUIRED_ACLS_DOC_URL + " for more info.",
          e
      );
    } catch (final Exception e) {
      throw new KsqlServerException("Could not get Kafka cluster configuration!", e);
    }
  }

  public static String getKafkaClusterId(final ServiceContext serviceContext) {
    try {
      return serviceContext.getAdminClient()
          .describeCluster()
          .clusterId()
          .get(DESCRIBE_CLUSTER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (final Exception e) {
      throw new RuntimeException("Failed to get Kafka cluster information", e);
    }
  }
}
