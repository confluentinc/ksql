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

package io.confluent.ksql.function;

import static java.util.Optional.empty;

import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udtf.UdtfDescription;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.security.ExtensionSecurityManager;
import io.confluent.ksql.util.KsqlConfig;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import org.apache.kafka.common.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinates the loading of UDFs, UDAFs and UDTFs. The actual loading of the functions is done in
 * of the specific function loader classes
 */
public class UserFunctionLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(UserFunctionLoader.class);

  private final File pluginDir;
  private final ClassLoader parentClassLoader;
  private final Predicate<String> blacklist;
  private final boolean loadCustomerUdfs;
  private final UdfLoader udfLoader;
  private final UdafLoader udafLoader;
  private final UdtfLoader udtfLoader;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public UserFunctionLoader(
      final MutableFunctionRegistry functionRegistry,
      final File pluginDir,
      final ClassLoader parentClassLoader,
      final Predicate<String> blacklist,
      final Optional<Metrics> metrics,
      final boolean loadCustomerUdfs
  ) {
    Objects.requireNonNull(functionRegistry, "functionRegistry can't be null");
    this.pluginDir = Objects.requireNonNull(pluginDir, "pluginDir can't be null");
    this.parentClassLoader = Objects.requireNonNull(
        parentClassLoader,
        "parentClassLoader can't be null"
    );
    this.blacklist = Objects.requireNonNull(blacklist, "blacklist can't be null");
    this.loadCustomerUdfs = loadCustomerUdfs;
    final SqlTypeParser typeParser = SqlTypeParser.create(TypeRegistry.EMPTY);
    this.udfLoader = new UdfLoader(functionRegistry, metrics, typeParser, false);
    this.udafLoader = new UdafLoader(functionRegistry, metrics, typeParser);
    this.udtfLoader = new UdtfLoader(functionRegistry, metrics, typeParser, false);
  }

  public void load() {
    // load functions packaged as part of ksql first
    loadFunctions(parentClassLoader, empty());
    if (loadCustomerUdfs) {
      try {
        if (!pluginDir.exists() && !pluginDir.isDirectory()) {
          LOGGER.info(
              "UDFs can't be loaded as as dir {} doesn't exist or is not a directory",
              pluginDir
          );
          return;
        }
        Files.find(pluginDir.toPath(), 1,
            (path, attributes) -> path.toString().endsWith(".jar")
        )
            .map(path -> UdfClassLoader.newClassLoader(path, parentClassLoader, blacklist))
            .forEach(classLoader ->
                loadFunctions(classLoader, Optional.of(classLoader.getJarPath())));
      } catch (final IOException e) {
        LOGGER.error("Failed to load UDFs from location {}", pluginDir, e);
      }
    }
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private void loadFunctions(final ClassLoader loader, final Optional<Path> path) {
    final String pathLoadedFrom = path.map(Path::toString).orElse(KsqlScalarFunction.INTERNAL_PATH);

    final ClassGraph classGraph = new ClassGraph();
    if (loader != parentClassLoader) {
      classGraph.overrideClassLoaders(loader);
    }

    try (ScanResult scan = classGraph
        .enableAnnotationInfo()
        .ignoreParentClassLoaders()
        .filterClasspathElements(ksqlEngineFilter(loader))
        .scan()
    ) {
      for (ClassInfo udf : scan.getClassesWithAnnotation(UdfDescription.class.getName())) {
        udfLoader.loadUdfFromClass(udf.loadClass(), pathLoadedFrom);
      }

      for (ClassInfo udaf : scan.getClassesWithAnnotation(UdafDescription.class.getName())) {
        udafLoader.loadUdafFromClass(udaf.loadClass(), pathLoadedFrom);
      }

      for (ClassInfo udtf : scan.getClassesWithAnnotation(UdtfDescription.class.getName())) {
        udtfLoader.loadUdtfFromClass(udtf.loadClass(), pathLoadedFrom);
      }
    }
  }

  private ClassGraph.ClasspathElementFilter ksqlEngineFilter(final ClassLoader loader) {
    // if we are loading from the parent classloader then restrict the name space to only
    // jars/dirs containing "ksql-engine". This is so we don't end up scanning every jar
    //return name -> parentClassLoader != loader || name.contains("ksqldb-engine");
    return name -> parentClassLoader != loader || name.contains("ksqldb-rest-app")
        || name.contains("ksqldb-engine");
  }

  public static UserFunctionLoader newInstance(
      final KsqlConfig config,
      final MutableFunctionRegistry metaStore,
      final String ksqlInstallDir
  ) {
    final boolean loadCustomerUdfs = config.getBoolean(KsqlConfig.KSQL_ENABLE_UDFS);
    final boolean collectMetrics = config.getBoolean(KsqlConfig.KSQL_COLLECT_UDF_METRICS);
    final String extDirName = config.getString(KsqlConfig.KSQL_EXT_DIR);
    final File pluginDir = KsqlConfig.DEFAULT_EXT_DIR.equals(extDirName)
        ? new File(ksqlInstallDir, extDirName)
        : new File(extDirName);

    final Optional<Metrics> metrics = collectMetrics
        ? Optional.of(MetricCollectors.getMetrics())
        : empty();

    if (config.getBoolean(KsqlConfig.KSQL_UDF_SECURITY_MANAGER_ENABLED)) {
      System.setSecurityManager(ExtensionSecurityManager.INSTANCE);
    }
    return new UserFunctionLoader(
        metaStore,
        pluginDir,
        Thread.currentThread().getContextClassLoader(),
        new Blacklist(new File(pluginDir, "resource-blacklist.txt")),
        metrics,
        loadCustomerUdfs
    );
  }
}
