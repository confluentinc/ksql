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

import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;

import java.io.File;

public final class CommandTopicBackupUtil {

  private CommandTopicBackupUtil() {
  }

  public static String backupLocation(final KsqlConfig ksqlConfig) {
    return ksqlConfig.getString(KsqlConfig.KSQL_METASTORE_BACKUP_LOCATION);
  }

  public static boolean commandTopicMissingWithValidBackup(
      final String commandTopic,
      final KafkaTopicClient kafkaTopicClient,
      final KsqlConfig ksqlConfig) {
    if (kafkaTopicClient.isTopicExists(commandTopic)) {
      return false;
    }

    String backupLocation = CommandTopicBackupUtil.backupLocation(ksqlConfig);
    if (!backupLocation.isEmpty()) {
      File backupDir = new File(backupLocation);
      if (backupDir.exists() && backupDir.isDirectory()) {
        return backupDir.listFiles().length > 0;
      }
    }
    return false;
  }
}
