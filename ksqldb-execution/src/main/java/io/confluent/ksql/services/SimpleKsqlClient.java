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
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public interface SimpleKsqlClient {

  /**
   * Send a request to remote Ksql server.
   * @param serverEndPoint the remote destination
   * @param sql the sql statement
   * @param requestProperties the request metadata provided by the server
   * @return the result of sql statement execution
   */
  RestResponse<KsqlEntityList> makeKsqlRequest(
      URI serverEndPoint,
      String sql,
      Map<String, ?> requestProperties
  );

  /**
   * Send pull query request to remote Ksql server.
   * @param serverEndPoint the remote destination
   * @param sql the pull query statement
   * @param configOverrides the config overrides provided by the client
   * @param requestProperties the request metadata provided by the server
   * @return the result of pull query evaluation
   */
  RestResponse<List<StreamedRow>> makeQueryRequest(
      URI serverEndPoint,
      String sql,
      Map<String, ?> configOverrides,
      Map<String, ?> requestProperties
  );

  /**
   * Send pull query request to remote Ksql server.  This version of makeQueryRequest allows
   * consuming the rows as they stream in rather than aggregating them all in one list.
   * @param serverEndPoint the remote destination
   * @param sql the pull query statement
   * @param configOverrides the config overrides provided by the client
   * @param requestProperties the request metadata provided by the server
   * @param rowConsumer A consumer that's fed lists of rows as they stream in
   * @return the number of rows returned by pull query
   */
  RestResponse<Integer> makeQueryRequest(
      URI serverEndPoint,
      String sql,
      Map<String, ?> configOverrides,
      Map<String, ?> requestProperties,
      Consumer<List<StreamedRow>> rowConsumer
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
   * Send a request to remote Ksql server to enquire about its view of the status of the cluster.
   *
   * @param serverEndPoint the remote destination.
   * @return response containing the cluster status.
   */
  RestResponse<ClusterStatusResponse> makeClusterStatusRequest(URI serverEndPoint);

  /**
   * Send a request to remote Ksql server to enquire about which state stores the remote
   * server maintains as an active and standby.
   *
   * @param serverEndPoint      the remote destination.
   * @param lagReportingMessage the host lag data
   */
  void makeAsyncLagReportRequest(
      URI serverEndPoint,
      LagReportingMessage lagReportingMessage
  );

  /*
  Close this client
   */
  void close();
}
