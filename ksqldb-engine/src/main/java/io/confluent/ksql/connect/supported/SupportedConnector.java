/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.connect.supported;

import io.confluent.ksql.connect.Connector;
import io.confluent.ksql.rest.entity.ConnectorInfo;
import java.util.Map;
import java.util.Optional;

/**
 * KSQL supports some "Blessed" connectors that we integrate well with. To be "blessed"
 * means that we can automatically import topics created by these connectors and that
 * we may provide templates that simplify configuration of these connectors.
 */
public interface SupportedConnector {

  /**
   * Constructs a {@link Connector} from the configuration given to us by connect.
   */
  Optional<Connector> fromConnectInfo(ConnectorInfo info);

  /**
   * Resolves a template configuration into something that connect can understand.
   */
  Map<String, String> resolveConfigs(Map<String, String> configs);

}
