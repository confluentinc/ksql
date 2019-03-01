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

package io.confluent.ksql.rest.ssl;

import java.util.Map;
import javax.ws.rs.client.ClientBuilder;

public interface SslClientConfigurer {

  /**
   * Configure the client's SSL settings based on the supplied {@code props}.
   *
   * @param builder the client builder to configure
   * @param props the props that drive what to configure.
   * @return the builder.
   */
  ClientBuilder configureSsl(ClientBuilder builder, Map<String, String> props);
}
