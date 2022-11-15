/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.metastore;

import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.AggregateFunctionInitArguments;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.KsqlTableFunction;
import io.confluent.ksql.function.TableFunctionFactory;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlReferentialIntegrityException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public final class MetaStoreImpl implements MutableMetaStore {

  private final Map<SourceName, SourceInfo> dataSources = new ConcurrentHashMap<>();
  private final Object referentialIntegrityLock = new Object();
  private final FunctionRegistry functionRegistry;
  private final TypeRegistry typeRegistry;

  public MetaStoreImpl(final FunctionRegistry functionRegistry) {
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    this.typeRegistry = new TypeRegistryImpl();
  }

  private MetaStoreImpl(
      final Map<SourceName, SourceInfo> dataSources,
      final FunctionRegistry functionRegistry,
      final TypeRegistry typeRegistry
  ) {
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    this.typeRegistry = new TypeRegistryImpl();

    dataSources.forEach((name, info) -> this.dataSources.put(name, info.copy()));
    typeRegistry.types()
        .forEachRemaining(type -> this.typeRegistry.registerType(type.getName(), type.getType()));
  }

  @Override
  public DataSource getSource(final SourceName sourceName) {
    final SourceInfo source = dataSources.get(sourceName);
    if (source == null) {
      return null;
    }
    return source.source;
  }

  @Override
  public void putSource(final DataSource dataSource, final boolean allowReplace) {
    final SourceInfo existing = dataSources.get(dataSource.getName());
    if (existing != null && !allowReplace) {
      final SourceName name = dataSource.getName();
      final String newType = dataSource.getDataSourceType().getKsqlType().toLowerCase();
      final String existingType = existing.source.getDataSourceType().getKsqlType().toLowerCase();

      throw new KsqlException(String.format(
          "Cannot add %s '%s': A %s with the same name already exists",
          newType, name.text(), existingType));
    } else if (existing != null) {
      existing.source.canUpgradeTo(dataSource).ifPresent(msg -> {
        throw new KsqlException("Cannot upgrade data source: " + msg);
      });
    }

    dataSources.put(dataSource.getName(), new SourceInfo(dataSource));
  }

  @Override
  public void deleteSource(final SourceName sourceName) {
    synchronized (referentialIntegrityLock) {
      dataSources.compute(sourceName, (ignored, source) -> {
        if (source == null) {
          throw new KsqlException(String.format("No data source with name %s exists.",
              sourceName.text()));
        }

        final String sourceForQueriesMessage = source.referentialIntegrity
            .getSourceForQueries()
            .stream()
            .collect(Collectors.joining(", "));

        final String sinkForQueriesMessage = source.referentialIntegrity
            .getSinkForQueries()
            .stream()
            .collect(Collectors.joining(", "));

        if (!sourceForQueriesMessage.isEmpty() || !sinkForQueriesMessage.isEmpty()) {
          throw new KsqlReferentialIntegrityException(
              String.format("Cannot drop %s.%n"
                      + "The following queries read from this source: [%s].%n"
                      + "The following queries write into this source: [%s].%n"
                      + "You need to terminate them before dropping %s.",
                  sourceName.toString(FormatOptions.noEscape()),
                  sourceForQueriesMessage,
                  sinkForQueriesMessage,
                  sourceName.toString(FormatOptions.noEscape())));
        }

        return null;
      });
    }
  }

  @Override
  public Map<SourceName, DataSource> getAllDataSources() {
    return dataSources
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().source));
  }

  @Override
  public void updateForPersistentQuery(
      final String queryId,
      final Set<SourceName> sourceNames,
      final Set<SourceName> sinkNames
  ) {
    synchronized (referentialIntegrityLock) {
      final String sourceAlreadyRegistered = streamSources(sourceNames)
          .filter(source -> source.referentialIntegrity.getSourceForQueries().contains(queryId))
          .map(source -> source.source.getName())
          .map(Object::toString)
          .collect(Collectors.joining(","));

      final String sinkAlreadyRegistered = streamSources(sinkNames)
          .filter(source -> source.referentialIntegrity.getSinkForQueries().contains(queryId))
          .map(source -> source.source.getName())
          .map(Object::toString)
          .collect(Collectors.joining(","));

      if (!sourceAlreadyRegistered.isEmpty() || !sinkAlreadyRegistered.isEmpty()) {
        throw new KsqlException("query already registered."
            + " queryId: " + queryId
            + ", registeredAgainstSource: " + sourceAlreadyRegistered
            + ", registeredAgainstSink: " + sinkAlreadyRegistered);
      }

      streamSources(sourceNames)
          .forEach(source -> source.referentialIntegrity.addSourceForQueries(queryId));
      streamSources(sinkNames)
          .forEach(source -> source.referentialIntegrity.addSinkForQueries(queryId));
    }
  }

  @Override
  public void removePersistentQuery(final String queryId) {
    synchronized (referentialIntegrityLock) {
      for (final SourceInfo sourceInfo : dataSources.values()) {
        sourceInfo.referentialIntegrity.removeQuery(queryId);
      }
    }
  }

  @Override
  public Set<String> getQueriesWithSource(final SourceName sourceName) {
    final SourceInfo sourceInfo = dataSources.get(sourceName);
    if (sourceInfo == null) {
      return Collections.emptySet();
    }
    return sourceInfo.referentialIntegrity.getSourceForQueries();
  }

  @Override
  public Set<String> getQueriesWithSink(final SourceName sourceName) {
    final SourceInfo sourceInfo = dataSources.get(sourceName);
    if (sourceInfo == null) {
      return Collections.emptySet();
    }
    return sourceInfo.referentialIntegrity.getSinkForQueries();
  }

  @Override
  public MutableMetaStore copy() {
    synchronized (referentialIntegrityLock) {
      return new MetaStoreImpl(dataSources, functionRegistry, typeRegistry);
    }
  }

  @Override
  public UdfFactory getUdfFactory(final FunctionName functionName) {
    return functionRegistry.getUdfFactory(functionName);
  }

  public boolean isAggregate(final FunctionName functionName) {
    return functionRegistry.isAggregate(functionName);
  }

  public boolean isTableFunction(final FunctionName functionName) {
    return functionRegistry.isTableFunction(functionName);
  }

  public KsqlAggregateFunction<?, ?, ?> getAggregateFunction(
      final FunctionName functionName,
      final SqlType argumentType,
      final AggregateFunctionInitArguments initArgs
  ) {
    return functionRegistry.getAggregateFunction(functionName, argumentType, initArgs);
  }

  public KsqlTableFunction getTableFunction(
      final FunctionName functionName,
      final List<SqlType> argumentTypes
  ) {
    return functionRegistry.getTableFunction(functionName, argumentTypes);
  }

  @Override
  public List<UdfFactory> listFunctions() {
    return functionRegistry.listFunctions();
  }

  @Override
  public AggregateFunctionFactory getAggregateFactory(final FunctionName functionName) {
    return functionRegistry.getAggregateFactory(functionName);
  }

  @Override
  public TableFunctionFactory getTableFunctionFactory(final FunctionName functionName) {
    return functionRegistry.getTableFunctionFactory(functionName);
  }

  @Override
  public List<AggregateFunctionFactory> listAggregateFunctions() {
    return functionRegistry.listAggregateFunctions();
  }

  @Override
  public List<TableFunctionFactory> listTableFunctions() {
    return functionRegistry.listTableFunctions();
  }

  private Stream<SourceInfo> streamSources(final Set<SourceName> sourceNames) {
    return sourceNames.stream()
        .map(sourceName -> {
          final SourceInfo sourceInfo = dataSources.get(sourceName);
          if (sourceInfo == null) {
            throw new KsqlException("Unknown source: " + sourceName.text());
          }

          return sourceInfo;
        });
  }

  @Override
  public boolean registerType(final String name, final SqlType type) {
    return typeRegistry.registerType(name, type);
  }

  @Override
  public boolean deleteType(final String name) {
    return typeRegistry.deleteType(name);
  }

  @Override
  public Optional<SqlType> resolveType(final String name) {
    return typeRegistry.resolveType(name);
  }

  @Override
  public Iterator<CustomType> types() {
    return typeRegistry.types();
  }

  private static final class SourceInfo {

    private final DataSource source;
    private final ReferentialIntegrityTableEntry referentialIntegrity;

    private SourceInfo(
        final DataSource source
    ) {
      this.source = Objects.requireNonNull(source, "source");
      this.referentialIntegrity = new ReferentialIntegrityTableEntry();
    }

    private SourceInfo(
        final DataSource source,
        final ReferentialIntegrityTableEntry referentialIntegrity
    ) {
      this.source = Objects.requireNonNull(source, "source");
      this.referentialIntegrity = referentialIntegrity.copy();
    }

    public SourceInfo copy() {
      return new SourceInfo(source, referentialIntegrity);
    }
  }
}
