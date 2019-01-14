/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.services;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.CreateAclsOptions;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.CreateDelegationTokenOptions;
import org.apache.kafka.clients.admin.CreateDelegationTokenResult;
import org.apache.kafka.clients.admin.CreatePartitionsOptions;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteAclsOptions;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DeleteRecordsOptions;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeAclsOptions;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeDelegationTokenOptions;
import org.apache.kafka.clients.admin.DescribeDelegationTokenResult;
import org.apache.kafka.clients.admin.DescribeLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ExpireDelegationTokenOptions;
import org.apache.kafka.clients.admin.ExpireDelegationTokenResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.RenewDelegationTokenOptions;
import org.apache.kafka.clients.admin.RenewDelegationTokenResult;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;

/**
 * An admin client to use while trying out operations.
 *
 * <p>The client will not make changes to the remote Kafka cluster.
 */
class TryAdminClient extends AdminClient {

  // Todo(ac): tidy up & test.

  private final AdminClient delegate;

  TryAdminClient(final AdminClient delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }

  @Override
  public void close(final long timeout, final TimeUnit unit) {
    delegate.close(timeout, unit);
  }

  @Override
  public CreateTopicsResult createTopics(
      final Collection<NewTopic> newTopics,
      final CreateTopicsOptions options
  ) {
    // todo(ac):
    // We might want a shared state between the different try kafka clients.
    throw new UnsupportedOperationException();
  }

  @Override
  public DeleteTopicsResult deleteTopics(
      final Collection<String> topics,
      final DeleteTopicsOptions options
  ) {
    // todo(ac):
    throw new UnsupportedOperationException();
  }

  @Override
  public ListTopicsResult listTopics(final ListTopicsOptions options) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DescribeTopicsResult describeTopics(
      final Collection<String> topics,
      final DescribeTopicsOptions options
  ) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DescribeClusterResult describeCluster(final DescribeClusterOptions options) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DescribeAclsResult describeAcls(
      final AclBindingFilter filter,
      final DescribeAclsOptions options
  ) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CreateAclsResult createAcls(
      final Collection<AclBinding> acls,
      final CreateAclsOptions options
  ) {
    // todo(ac):
    throw new UnsupportedOperationException();
  }

  @Override
  public DeleteAclsResult deleteAcls(
      final Collection<AclBindingFilter> acls,
      final DeleteAclsOptions options
  ) {
    // todo(ac):
    throw new UnsupportedOperationException();
  }

  @Override
  public DescribeConfigsResult describeConfigs(
      final Collection<ConfigResource> configs,
      final DescribeConfigsOptions options
  ) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AlterConfigsResult alterConfigs(
      final Map<ConfigResource, Config> configs,
      final AlterConfigsOptions options
  ) {
    // todo(ac):
    throw new UnsupportedOperationException();
  }

  @Override
  public AlterReplicaLogDirsResult alterReplicaLogDirs(
      final Map<TopicPartitionReplica, String> replicaAssignment,
      final AlterReplicaLogDirsOptions options
  ) {
    // todo(ac):
    throw new UnsupportedOperationException();
  }

  @Override
  public DescribeLogDirsResult describeLogDirs(
      final Collection<Integer> brokers,
      final DescribeLogDirsOptions options
  ) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DescribeReplicaLogDirsResult describeReplicaLogDirs(
      final Collection<TopicPartitionReplica> replicas,
      final DescribeReplicaLogDirsOptions options
  ) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CreatePartitionsResult createPartitions(
      final Map<String, NewPartitions> newPartitions,
      final CreatePartitionsOptions options
  ) {
    // todo(ac):
    throw new UnsupportedOperationException();
  }

  @Override
  public DeleteRecordsResult deleteRecords(
      final Map<TopicPartition, RecordsToDelete> recordsToDelete,
      final DeleteRecordsOptions options
  ) {
    // todo(ac):
    throw new UnsupportedOperationException();
  }

  @Override
  public CreateDelegationTokenResult createDelegationToken(
      final CreateDelegationTokenOptions options
  ) {
    // todo(ac):
    throw new UnsupportedOperationException();
  }

  @Override
  public RenewDelegationTokenResult renewDelegationToken(
      final byte[] hmac,
      final RenewDelegationTokenOptions options
  ) {
    // todo(ac):
    throw new UnsupportedOperationException();
  }

  @Override
  public ExpireDelegationTokenResult expireDelegationToken(
      final byte[] hmac,
      final ExpireDelegationTokenOptions options
  ) {
    // todo(ac):
    throw new UnsupportedOperationException();
  }

  @Override
  public DescribeDelegationTokenResult describeDelegationToken(
      final DescribeDelegationTokenOptions options
  ) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DescribeConsumerGroupsResult describeConsumerGroups(
      final Collection<String> groupIds,
      final DescribeConsumerGroupsOptions options
  ) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListConsumerGroupsResult listConsumerGroups(final ListConsumerGroupsOptions options) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(
      final String groupId,
      final ListConsumerGroupOffsetsOptions options
  ) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DeleteConsumerGroupsResult deleteConsumerGroups(
      final Collection<String> groupIds,
      final DeleteConsumerGroupsOptions options
  ) {
    // todo(ac):
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return delegate.metrics();
  }
}
