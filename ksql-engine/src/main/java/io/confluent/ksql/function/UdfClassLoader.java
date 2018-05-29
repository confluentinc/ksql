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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Objects;

import io.confluent.ksql.util.KsqlException;

public class UdfClassLoader extends URLClassLoader {
  private static final Logger logger = LoggerFactory.getLogger(URLClassLoader.class);
  private final Blacklister blacklister;
  private final Path path;

  private UdfClassLoader(final Path path,
                         final URL[] urls,
                         final ClassLoader parent,
                         final Blacklister blacklister) {
    super(urls, parent);
    this.blacklister = Objects.requireNonNull(blacklister, "blacklister can't be null");
    this.path = Objects.requireNonNull(path, "path can't be null");
  }

  @Override
  protected Class<?> loadClass(final String name, final boolean resolve)
      throws ClassNotFoundException {
    if (blacklister.blacklisted(name)) {
      throw new ClassNotFoundException("The requested class is not permitted to be used from a "
          + "udf. Class " + name);
    }
    Class<?> clazz = findLoadedClass(name);
    if (clazz == null) {
      try {
        if (shouldLoadFromChild(name)) {
          clazz = findClass(name);
        }
      } catch (ClassNotFoundException e) {
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
    return clazz;
  }

  private boolean shouldLoadFromChild(final String name) {
    return !name.startsWith("io.confluent");
  }

  static UdfClassLoader newClassLoader(final Path path,
                                       final ClassLoader parentClassLoader,
                                       final Blacklister blacklister) {
    logger.debug("creating UdfClassLoader for {}", path);
    return AccessController.doPrivileged(
        (PrivilegedAction<UdfClassLoader>) () ->
            new UdfClassLoader(path, toUrl(path), parentClassLoader, blacklister));

  }

  private static URL[] toUrl(Path path) {
    try {
      return new URL[]{path.toUri().toURL()};
    } catch (MalformedURLException e) {
      throw new KsqlException("Unable to create classloader for path:" + path, e);
    }
  }

}
