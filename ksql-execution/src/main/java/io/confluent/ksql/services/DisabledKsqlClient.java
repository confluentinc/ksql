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
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.StreamedRow;
import java.net.URI;
import java.util.List;

/**
 * A KSQL client implementation for use when communication with other nodes is not supported.
 */
public final class DisabledKsqlClient implements SimpleKsqlClient {

  public static DisabledKsqlClient instance() {
    return new DisabledKsqlClient();
  }

  private DisabledKsqlClient() {
  }

  @Override
  public RestResponse<KsqlEntityList> makeKsqlRequest(URI serverEndPoint, String sql) {
    throw new UnsupportedOperationException("KSQL client is disabled");
  }

  @Override
  public RestResponse<List<StreamedRow>> makeQueryRequest(URI serverEndPoint, String sql) {
    throw new UnsupportedOperationException("KSQL client is disabled");
  }
}

