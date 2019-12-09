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

import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.StreamedRow;
import java.net.URI;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.kafka.streams.state.HostInfo;

@ThreadSafe
public interface SimpleKsqlClient {

  RestResponse<KsqlEntityList> makeKsqlRequest(
      URI serverEndPoint,
      String sql
  );

  RestResponse<List<StreamedRow>> makeQueryRequest(
      URI serverEndPoint,
      String sql
  );

  /**
   * Send heartbeat to remote Ksql server.
   * @param serverEndPoint the remote destination.
   * @param host the host information of the sender.
   * @param timestamp the timestamp the heartbeat is sent.
   */
  void makeAsyncHeartbeatRequest(
      URI serverEndPoint,
      HostInfo host,
      long timestamp
  );

  /**
   * Send a request to remote Ksql server to inquire about its view of the status of the cluster.
   * @param serverEndPoint the remote destination.
   * @return response containing the cluster status.
   */
  RestResponse<ClusterStatusResponse> makeClusterStatusRequest(URI serverEndPoint);
}
