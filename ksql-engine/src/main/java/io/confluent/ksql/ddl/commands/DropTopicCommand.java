/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.ddl.commands;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.DropTopic;


public class DropTopicCommand implements DdlCommand {

  private final String topicName;

  public DropTopicCommand(final DropTopic dropTopic) {
    this.topicName = dropTopic.getTopicName().getSuffix();
  }

  DropTopicCommand(final String topicName) {
    this.topicName = topicName;
  }

  @Override
  public DdlCommandResult run(final MetaStore metaStore) {
    metaStore.deleteTopic(topicName);
    return new DdlCommandResult(true, "Topic " + topicName + " was dropped");
  }
}
