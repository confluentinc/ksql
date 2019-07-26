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

package io.confluent.ksql.topic;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.parser.DefaultTraversalVisitor;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AstNode;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.util.KsqlException;
import java.util.HashSet;
import java.util.Set;

/**
 * Helper class that extracts all source topics from a query node.
 */
public class SourceTopicsExtractor extends DefaultTraversalVisitor<AstNode, Void> {
  private final Set<String> sourceTopics = new HashSet<>();
  private final MetaStore metaStore;

  private String primaryKafkaTopicName = null;

  public SourceTopicsExtractor(final MetaStore metaStore) {
    this.metaStore = metaStore;
  }

  public String getPrimaryKafkaTopicName() {
    return primaryKafkaTopicName;
  }

  public Set<String> getSourceTopics() {
    return sourceTopics;
  }

  @Override
  protected AstNode visitJoin(final Join node, final Void context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);
    return null;
  }

  @Override
  protected AstNode visitAliasedRelation(final AliasedRelation node, final Void context) {
    final String structuredDataSourceName = ((Table) node.getRelation()).getName().getSuffix();
    final DataSource<?> source = metaStore.getSource(structuredDataSourceName);
    if (source == null) {
      throw new KsqlException(structuredDataSourceName + " does not exist.");
    }

    // This method is called first with the primary kafka topic (or the node.getFrom() node)
    if (primaryKafkaTopicName == null) {
      primaryKafkaTopicName = source.getKafkaTopicName();
    }

    sourceTopics.add(source.getKafkaTopicName());
    return node;
  }
}
