/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.function;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.confluent.ksql.function.udf.PluggableUdf;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;

public class UdfLoader {

  private static final Logger logger = LoggerFactory.getLogger(UdfLoader.class);

  private final MetaStore metaStore;
  private final File pluginDir;
  private final ClassLoader parentClassLoader;
  private final Predicate<String> blacklist;
  private final UdfCompiler compiler;
  private final boolean loadCustomerUdfs;


  public UdfLoader(final MetaStore metaStore,
                   final File pluginDir,
                   final ClassLoader parentClassLoader,
                   final Predicate<String> blacklist,
                   final UdfCompiler compiler,
                   final boolean loadCustomerUdfs) {
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore can't be null");
    this.pluginDir = Objects.requireNonNull(pluginDir, "pluginDir can't be null");
    this.parentClassLoader = Objects.requireNonNull(parentClassLoader,
        "parentClassLoader can't be null");
    this.blacklist = Objects.requireNonNull(blacklist, "blacklist can't be null");
    this.compiler = Objects.requireNonNull(compiler, "compiler can't be null");
    this.loadCustomerUdfs = loadCustomerUdfs;
  }

  public void load() {
    // load udfs packaged as part of ksql first
    loadUdfs(parentClassLoader);
    if (loadCustomerUdfs) {
      try {
        Files.find(pluginDir.toPath(), 1, (path, attributes) -> path.toString().endsWith(".jar"))
            .map(path -> UdfClassLoader.newClassLoader(path, parentClassLoader, blacklist))
            .forEach(this::loadUdfs);
      } catch (IOException e) {
        logger.error("Failed to load UDFs from location {}", pluginDir, e);
      }
    }
  }


  private void loadUdfs(final ClassLoader loader) {
    new FastClasspathScanner()
        .overrideClassLoaders(loader)
        .ignoreParentClassLoaders()
        // if we are loading from the parent classloader then restrict the name space to only
        // jars/dirs containing "ksql-engine". This is so we don't end up scanning every jar
        .filterClasspathElements(
            name -> parentClassLoader != loader || name.contains("ksql-engine"))
        .matchClassesWithMethodAnnotation(Udf.class,
            (theClass, executable) -> {
              final UdfDescription annotation = theClass.getAnnotation(UdfDescription.class);
              if (annotation != null) {
                final Method method = (Method) executable;
                try {
                  final UdfInvoker udf = compiler.compile(method, loader);
                  addFunction(annotation, method, udf);
                } catch (final KsqlException e) {
                  if (parentClassLoader == loader) {
                    throw e;
                  } else {
                    logger.warn("Failed to add UDF to the MetaStore. name={} method={}",
                        annotation.name(),
                        method,
                        e);
                  }
                }
              }
            })
        .scan();
  }

  private void addFunction(final UdfDescription annotation,
                           final Method method,
                           final UdfInvoker udf) {
    metaStore.addFunction(new KsqlFunction(
        SchemaUtil.getSchemaFromType(method.getReturnType()),
        Arrays.stream(method.getGenericParameterTypes())
            .map(SchemaUtil::getSchemaFromType).collect(Collectors.toList()),
        annotation.name(),
        PluggableUdf.class,
        () -> {
          try {
            return new PluggableUdf(udf, method.getDeclaringClass().newInstance());
          } catch (Exception e) {
            throw new KsqlException("Failed to create instance for UDF="
                + annotation.name()
                + ", method=" + method,
                e);
          }
        }));
  }

  public static UdfLoader newInstance(final KsqlConfig config,
                                      final MetaStore metaStore,
                                      final String ksqlInstallDir
  ) {
    final Boolean loadCustomerUdfs = config.getBoolean(KsqlConfig.KSQL_ENABLE_UDFS);
    final File pluginDir = new File(ksqlInstallDir, "ext");

    Preconditions.checkArgument(!loadCustomerUdfs || pluginDir.isDirectory(),
        pluginDir.getPath() + " must be a directory when " + KsqlConfig.KSQL_ENABLE_UDFS
        + " is true");
    return new UdfLoader(metaStore,
        pluginDir,
        Thread.currentThread().getContextClassLoader(),
        new Blacklist(new File(pluginDir, "resource-blacklist.txt")),
        new UdfCompiler(),
        loadCustomerUdfs);
  }

}
