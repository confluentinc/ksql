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

package io.confluent.ksql.security;

import java.security.AllPermission;
import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.util.Stack;

import io.confluent.ksql.function.udf.PluggableUdf;

/**
 * A simple security manger extension to block UDFs from performing potentially malicious
 * actions, such as System.exit, executing processes, connecting to and reading from sockets etc
 */
public class ExtensionSecurityManager extends SecurityManager {

  public static final ExtensionSecurityManager INSTANCE
      = new ExtensionSecurityManager();
  private static final ThreadLocal<Stack<Boolean>> UDF_IS_EXECUTING = new ThreadLocal<>();

  // so only can be accessed via the INSTANCE
  private ExtensionSecurityManager() {
    Policy.setPolicy(new Policy() {
      @Override
      public PermissionCollection getPermissions(final CodeSource codesource) {
        final Permissions permissions = new Permissions();
        if (codesource == null || codesource.getLocation() == null) {
          return permissions;
        }

        // allow allowing of classes and jars from the filesystem
        if (codesource.getLocation().getProtocol().equals("file")) {
          permissions.add(new AllPermission());
        }
        return permissions;
      }

      @Override
      public PermissionCollection getPermissions(final ProtectionDomain domain) {
        return getPermissions(domain.getCodeSource());
      }

      @Override
      public boolean implies(final ProtectionDomain domain, final Permission permission) {
        final CodeSource codeSource = domain.getCodeSource();
        if (codeSource == null || codeSource.getLocation() == null) {
          return false;
        }

        return codeSource.getLocation().getProtocol().equals("file");

      }
    });
  }

  public synchronized void pushInUdf() {
    if (callerIsAllowed()) {
      if (UDF_IS_EXECUTING.get() == null) {
        UDF_IS_EXECUTING.set(new Stack<>());
      }
      UDF_IS_EXECUTING.get().push(true);
    }
  }

  public void popInUdf() {
    if (callerIsAllowed()) {
      final Stack<Boolean> stack = UDF_IS_EXECUTING.get();
      if (stack != null && !stack.isEmpty()) {
        UDF_IS_EXECUTING.get().pop();
      }
    }
  }

  @Override
  public void checkExit(final int status) {
    if (inUdfExecution()) {
      throw new SecurityException("A UDF attempted to call System.exit");
    }
    super.checkExit(status);
  }

  private boolean inUdfExecution() {
    final Stack<Boolean> executing = UDF_IS_EXECUTING.get();
    return executing != null && !executing.isEmpty();
  }

  @Override
  public void checkExec(final String cmd) {
    if (inUdfExecution()) {
      throw new SecurityException("A UDF attempted to execute the following cmd: " + cmd);
    }
    super.checkExec(cmd);
  }

  @Override
  public void checkPrintJobAccess() {
    if (inUdfExecution()) {
      throw new SecurityException("A UDF attempted to create a Print Job");
    }
    super.checkPrintJobAccess();
  }

  @Override
  public void checkConnect(final String host, final int port) {
    if (inUdfExecution()) {
      throw new SecurityException("A UDF attempted a socket connection to host='" + host
          + "' port=" + port);
    }
    super.checkConnect(host, port);
  }

  @Override
  public void checkConnect(final String host, final int port, final Object context) {
    if (inUdfExecution()) {
      throw new SecurityException("A UDF attempted a socket connection to host='" + host
          + "' port=" + port);
    }
    super.checkConnect(host, port, context);
  }

  @Override
  public void checkListen(final int port) {
    if (inUdfExecution()) {
      throw new SecurityException("A UDF attempted to listen on  port=" + port);
    }
    super.checkListen(port);
  }

  @Override
  public void checkAccept(final String host, final int port) {
    if (inUdfExecution()) {
      throw new SecurityException("A UDF attempted to accept a socket connection to host='" + host
          + "' port=" + port);
    }
    super.checkAccept(host, port);
  }


  /**
   * Check if the caller is a PluggableUdf. It will be the third
   * item in the class array.
   * @return true if caller is allowed
   */
  private boolean callerIsAllowed() {
    return getClassContext()[2].equals(PluggableUdf.class);
  }
}
