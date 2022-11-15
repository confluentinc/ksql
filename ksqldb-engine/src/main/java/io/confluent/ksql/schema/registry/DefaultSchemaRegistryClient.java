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

package io.confluent.ksql.schema.registry;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlSchemaRegistryNotConfiguredException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** 
 * Implements the SchemaRegistryClient interface. Used as default when the Schema Registry URL isn't
 * specified in {@link io.confluent.ksql.util.KsqlConfig}
 */
public class DefaultSchemaRegistryClient implements SchemaRegistryClient {
  
  public static final String SCHEMA_REGISTRY_CONFIG_NOT_SET =
      "KSQL is not configured to use a schema registry. To enable it, please set "
          + KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY;
  
  private final KsqlSchemaRegistryNotConfiguredException configException;

  public DefaultSchemaRegistryClient() {
    configException =
        new KsqlSchemaRegistryNotConfiguredException(SCHEMA_REGISTRY_CONFIG_NOT_SET);
  }

  @Override
  public Optional<ParsedSchema> parseSchema(
      final String var1, final String var2,
      final List<SchemaReference> var3
  ) {
    throw configException;
  }

  @Override
  public int register(final String s, final ParsedSchema parsedSchema) {
    throw configException;
  }

  @Override
  public int register(final String s, final ParsedSchema parsedSchema, final int i, final int i1) {
    throw configException;
  }

  @Override
  public ParsedSchema getSchemaById(final int i) {
    throw configException;
  }

  @Override
  public ParsedSchema getSchemaBySubjectAndId(final String s, final int i) {
    throw configException;
  }

  @Override
  public Collection<String> getAllSubjectsById(final int i) {
    throw configException;
  }

  @Override
  public SchemaMetadata getLatestSchemaMetadata(final String s) {
    throw configException;
  }

  @Override
  public SchemaMetadata getSchemaMetadata(final String s, final int i) {
    throw configException;
  }

  @Override
  public int getVersion(final String s, final ParsedSchema parsedSchema) {
    throw configException;
  }

  @Override
  public List<Integer> getAllVersions(final String s) {
    throw configException;
  }

  @Override
  public boolean testCompatibility(final String s, final ParsedSchema parsedSchema) {
    throw configException;
  }

  @Override
  public String updateCompatibility(final String s, final String s1) {
    throw configException;
  }

  @Override
  public String getCompatibility(final String s) {
    throw configException;
  }

  @Override
  public String setMode(final String s) {
    throw configException;
  }

  @Override
  public String setMode(final String s, final String s1) {
    throw configException;
  }

  @Override
  public String getMode() {
    throw configException;
  }

  @Override
  public String getMode(final String s) {
    throw configException;
  }

  @Override
  public Collection<String> getAllSubjects() {
    return Collections.emptyList();
  }

  @Override
  public int getId(final String s, final ParsedSchema parsedSchema) {
    throw configException;
  }

  @Override
  public List<Integer> deleteSubject(final String s) {
    return ImmutableList.of();
  }

  @Override
  public List<Integer> deleteSubject(final String s, final boolean b) {
    return ImmutableList.of();
  }

  @Override
  public List<Integer> deleteSubject(final Map<String, String> map, final String s) {
    return ImmutableList.of();
  }

  @Override
  public Integer deleteSchemaVersion(final String s, final String s1) {
    throw configException;
  }

  @Override
  public Integer deleteSchemaVersion(
      final Map<String, String> map, 
      final String s, 
      final String s1
  ) {
    throw configException;
  }

  @Override
  public void reset() {
    throw configException;
  }
}
