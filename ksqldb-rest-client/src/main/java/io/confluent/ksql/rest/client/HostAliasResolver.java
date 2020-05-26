/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.rest.client;

/**
 * Interface for resolving aliases to a given host.  This is useful in testing, for example as it
 * allows domains to resolve to localhost, while still passing on the original hostname over http.
 */
public interface HostAliasResolver {

  /**
   * Resolves the given alias to another host.
   * @param alias The alias hostname.
   * @return The resolved hostname which will actually be contacted.
   */
  String resolve(String alias);
}
