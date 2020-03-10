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
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.util.KsqlHostInfo;
import java.net.URI;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public interface SimpleKsqlClient {

  RestResponse<KsqlEntityList> makeKsqlRequest(
      URI serverEndPoint,
      String sql
  );

  RestResponse<List<StreamedRow>> makeQueryRequest(
      URI serverEndPoint,
      String sql,
      Map<String, ?> properties
  );

  /**
   * Send heartbeat to remote Ksql server.
   * @param serverEndPoint the remote destination.
   * @param host the host information of the sender.
   * @param timestamp the timestamp the heartbeat is sent.
   */
  void makeAsyncHeartbeatRequest(
      URI serverEndPoint,
      KsqlHostInfo host,
      long timestamp
  );

  /**
   * Send a request to remote Ksql server to inquire about its view of the status of the cluster.
   * @param serverEndPoint the remote destination.
   * @return response containing the cluster status.
   */
  RestResponse<ClusterStatusResponse> makeClusterStatusRequest(URI serverEndPoint);

  /**
   * Send a request to remote Ksql server to inquire to inquire about which state stores the
   * remote server maintains as an active and standby.
   * @param serverEndPoint the remote destination.
   * @param lagReportingMessage the host lag data
   */
  void makeAsyncLagReportRequest(
      URI serverEndPoint,
      LagReportingMessage lagReportingMessage
  );
}
