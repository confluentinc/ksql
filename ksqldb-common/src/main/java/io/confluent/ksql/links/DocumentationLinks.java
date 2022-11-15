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

package io.confluent.ksql.links;

public final class DocumentationLinks {

  private static final String CONFLUENT_DOCS_ROOT_URL = "https://docs.confluent.io/current/";

  /*
  KSQL
   */
  private static final String KSQL_DOCS_ROOT_URL = "https://docs.ksqldb.io/en/latest/";

  private static final String SECURITY_DOCS_URL = KSQL_DOCS_ROOT_URL
      + "operate-and-deploy/installation/server-config/security/";

  public static final String SECURITY_CLI_SSL_DOC_URL = SECURITY_DOCS_URL
      + "#configure-the-cli-for-https";

  public static final String SECURITY_REQUIRED_ACLS_DOC_URL = SECURITY_DOCS_URL
      + "#required-acls";

  public static final String SYNTHETIC_JOIN_KEY_DOC_URL = "https://cnfl.io/2LV7ouS";

  public static final String PUSH_PULL_QUERY_DOC_LINK = "https://cnfl.io/queries";

  /*
  Schema Registry
   */
  private static final String SCHEMA_REGISTRY_DOCS_ROOT_URL = CONFLUENT_DOCS_ROOT_URL
      + "schema-registry/docs/";

  public static final String SR_SERIALISER_DOC_URL = SCHEMA_REGISTRY_DOCS_ROOT_URL
      + "serializer-formatter.html";

  private static final String SCHEMA_REGISTRY_API_DOC_URL =
      SCHEMA_REGISTRY_DOCS_ROOT_URL + "api.html";

  public static final String SR_REST_GETSUBJECTS_DOC_URL = SCHEMA_REGISTRY_API_DOC_URL
      + "#get--subjects";

  public static final String SCHEMA_REGISTRY_SECURITY_DOC_URL = SCHEMA_REGISTRY_DOCS_ROOT_URL
      + "security.html";

  private DocumentationLinks() {
  }
}
