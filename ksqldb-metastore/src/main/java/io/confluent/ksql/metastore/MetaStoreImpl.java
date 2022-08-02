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

import com.google.common.collect.Iterables;
import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlTableFunction;
import io.confluent.ksql.function.TableFunctionFactory;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlReferentialIntegrityException;
import io.vertx.core.impl.ConcurrentHashSet;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public final class MetaStoreImpl implements MutableMetaStore {
  private static final Logger LOG = LoggerFactory.getLogger(MetaStoreImpl.class);

  // these sources have a constraint that cannot be deleted until the references are dropped first
  private final Map<SourceName, Set<SourceName>> dropConstraints = new ConcurrentHashMap<>();

  private final Map<SourceName, SourceInfo> dataSources = new ConcurrentHashMap<>();
  private final Object metaStoreLock = new Object();
  private final FunctionRegistry functionRegistry;
  private final TypeRegistry typeRegistry;

  public MetaStoreImpl(final FunctionRegistry functionRegistry) {
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    this.typeRegistry = new TypeRegistryImpl();
  }

  private MetaStoreImpl(
      final Map<SourceName, SourceInfo> dataSources,
      final FunctionRegistry functionRegistry,
      final TypeRegistry typeRegistry,
      final Map<SourceName, Set<SourceName>> dropConstraints
  ) {
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    this.typeRegistry = new TypeRegistryImpl();

    dataSources.forEach((name, info) -> this.dataSources.put(name, info.copy()));
    typeRegistry.types()
        .forEachRemaining(type -> this.typeRegistry.registerType(type.getName(), type.getType()));

    dropConstraints.forEach((source, references) -> {
      final Set<SourceName> childSources = new ConcurrentHashSet<>();
      childSources.addAll(references);
      this.dropConstraints.put(source, childSources);
    });
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

    // Replace the dataSource if one exists, which may contain changes in the Schema, with
    // a copy of the previous source info
    dataSources.put(dataSource.getName(),
        (existing != null) ? existing.copyWith(dataSource) : new SourceInfo(dataSource));

    LOG.info("Source {} created on the metastore", dataSource.getName().text());

    // Re-build the DROP constraints if existing sources have references to this new source.
    // This logic makes sure that drop constraints are set back if sources were deleted during
    // the metastore restoration (See deleteSource()).
    dataSources.forEach((name, info) -> {
      info.references.forEach(ref -> {
        if (ref.equals(dataSource.getName())) {
          LOG.debug("Add a drop constraint reference back to source '{}' from source '{}'",
              dataSource.getName().text(), name.text());

          addConstraint(dataSource.getName(), name);
        }
      });
    });
  }

  @Override
  public void deleteSource(final SourceName sourceName, final boolean restoreInProgress) {
    synchronized (metaStoreLock) {
      dataSources.compute(sourceName, (ignored, sourceInfo) -> {
        if (sourceInfo == null) {
          throw new KsqlException(String.format("No data source with name %s exists.",
              sourceName.text()));
        }

        if (dropConstraints.containsKey(sourceName)) {
          final String references = dropConstraints.get(sourceName).stream().map(SourceName::text)
              .sorted().collect(Collectors.joining(", "));

          // If this request is part of the metastore restoration process, then ignore any
          // constraints this source may have. This logic fixes a compatibility issue caused by
          // https://github.com/confluentinc/ksql/pull/6545, which makes the restoration to fail if
          // this source has another source referencing to it.
          if (restoreInProgress) {
            LOG.warn("The following streams and/or tables read from the '{}' source: [{}].\n"
                    + "Ignoring DROP constraints when restoring the metastore. \n"
                    + "Future CREATE statements that recreate this '{}' source may not have "
                    + "DROP constraints if existing source references exist.",
                sourceName.text(), references);

            dropConstraints.remove(sourceName);
          } else {
            throw new KsqlReferentialIntegrityException(String.format(
                "Cannot drop %s.%n"
                    + "The following streams and/or tables read from this source: [%s].%n"
                    + "You need to drop them before dropping %s.",
                sourceName.text(),
                references,
                sourceName.text()
            ));
          }
        }

        // Remove drop constraints from the referenced sources
        sourceInfo.references.stream().forEach(ref -> dropConstraint(ref, sourceName));

        LOG.info("Source {} deleted from the metastore", sourceName.text());
        return null;
      });
    }
  }

  @Override
  public void addSourceReferences(
      final SourceName sourceName,
      final Set<SourceName> sourceReferences
  ) {
    synchronized (metaStoreLock) {
      if (sourceReferences.contains(sourceName)) {
        throw new KsqlException(String.format("Source name '%s' should not be referenced itself.",
            sourceName.text()));
      }

      Iterables.concat(Collections.singleton(sourceName), sourceReferences).forEach(name -> {
        if (!dataSources.containsKey(name)) {
          throw new KsqlException(
              String.format("No data source with name '%s' exists.", name.text())
          );
        }
      });

      // add a constraint to the referenced sources to prevent deleting them
      sourceReferences.forEach(s -> addConstraint(s, sourceName));

      // add all references to the source
      dataSources.get(sourceName).references.addAll(sourceReferences);
    }
  }

  Set<SourceName> getSourceReferences(final SourceName sourceName) {
    final SourceInfo sourceInfo = dataSources.get(sourceName);
    if (sourceInfo == null) {
      return Collections.emptySet();
    }

    return sourceInfo.references;
  }

  private void addConstraint(final SourceName source, final SourceName sourceWithReference) {
    dropConstraints.computeIfAbsent(source, x -> ConcurrentHashMap.newKeySet())
        .add(sourceWithReference);
  }

  private void dropConstraint(final SourceName source, final SourceName sourceWithReference) {
    dropConstraints.computeIfPresent(source, (k , info) -> {
      info.remove(sourceWithReference);
      return (info.isEmpty()) ? null : info;
    });
  }

  @Override
  public Set<SourceName> getSourceConstraints(final SourceName sourceName) {
    return dropConstraints.getOrDefault(sourceName, Collections.emptySet());
  }

  @Override
  public Map<SourceName, DataSource> getAllDataSources() {
    return dataSources
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().source));
  }

  @Override
  public MutableMetaStore copy() {
    synchronized (metaStoreLock) {
      return new MetaStoreImpl(dataSources, functionRegistry, typeRegistry, dropConstraints);
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

  public boolean isPresent(final FunctionName functionName) {
    return functionRegistry.isPresent(functionName);
  }

  public KsqlTableFunction getTableFunction(
      final FunctionName functionName,
      final List<SqlArgument> argumentTypes
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

    // parent sources that this source references to; it is used to remove constraints from
    // the parent table when this source is deleted
    private final Set<SourceName> references = new ConcurrentHashSet<>();

    private SourceInfo(
        final DataSource source
    ) {
      this.source = Objects.requireNonNull(source, "source");
    }

    private SourceInfo(
        final DataSource source,
        final Set<SourceName> references
    ) {
      this.source = Objects.requireNonNull(source, "source");
      this.references.addAll(
          Objects.requireNonNull(references, "references"));
    }

    public SourceInfo copy() {
      return copyWith(source);
    }

    public SourceInfo copyWith(final DataSource source) {
      return new SourceInfo(source, references);
    }
  }
}
