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

import io.confluent.ksql.util.KsqlException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Objects;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class UdfClassLoader extends URLClassLoader {
  private static final Logger logger = LoggerFactory.getLogger(URLClassLoader.class);
  private final Predicate<String> blacklist;
  private final Path path;

  private UdfClassLoader(final Path path,
                         final URL[] urls,
                         final ClassLoader parent,
                         final Predicate<String> blacklist) {
    super(urls, parent);
    this.blacklist = Objects.requireNonNull(blacklist, "blacklist can't be null");
    this.path = Objects.requireNonNull(path, "path can't be null");
  }

  @Override
  protected Class<?> loadClass(final String name, final boolean resolve)
      throws ClassNotFoundException {
    logger.info("Loading class:- {}", name);
    if (blacklist.test(name)) {
      throw new ClassNotFoundException("The requested class is not permitted to be used from a "
          + "udf. Class " + name);
    }
    Class<?> clazz = findLoadedClass(name);
    logger.info("Found clazz:- {}", clazz);
    if (clazz == null) {
      try {
        if (shouldLoadFromChild(name)) {
          clazz = findClass(name);
        }
      } catch (final ClassNotFoundException e) {
        logger.trace("Class {} not found in {} using parent classloader",
            name,
            path);
      }
    }
    if (clazz == null) {
      clazz =  super.loadClass(name, false);
    }
    if (resolve) {
      resolveClass(clazz);
    }
    logger.info("Final clazz value:- {}", clazz);
    return clazz;
  }

  private boolean shouldLoadFromChild(final String name) {
    return !name.startsWith("io.confluent")
        && !name.startsWith("org.apache.kafka");
  }

  static UdfClassLoader newClassLoader(final Path path,
                                       final ClassLoader parentClassLoader,
                                       final Predicate<String> blacklist) {
    logger.debug("creating UdfClassLoader for {}", path);
    return AccessController.doPrivileged(
        (PrivilegedAction<UdfClassLoader>) () ->
            new UdfClassLoader(path, toUrl(path), parentClassLoader, blacklist));

  }

  private static URL[] toUrl(final Path path) {
    try {
      return new URL[]{path.toUri().toURL()};
    } catch (final MalformedURLException e) {
      throw new KsqlException("Unable to create classloader for path:" + path, e);
    }
  }

  Path getJarPath() {
    return path;
  }
}
