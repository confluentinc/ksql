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
 * A simple security manager extension to block UDFs from calling
 * System.exit or executing processes
 */
public class ExtensionSecurityManager extends SecurityManager {

  public static final ExtensionSecurityManager INSTANCE
      = new ExtensionSecurityManager();
  private static final ThreadLocal<Stack<Boolean>> UDF_IS_EXECUTING = new ThreadLocal<>();

  // so only can be accessed via the INSTANCE
  private ExtensionSecurityManager() {
    // set a policy here that grants all permissions
    Policy.setPolicy(new Policy() {
      @Override
      public PermissionCollection getPermissions(final CodeSource codesource) {
        final Permissions permissions = new Permissions();
        permissions.add(new AllPermission());
        return permissions;
      }

      @Override
      public PermissionCollection getPermissions(final ProtectionDomain domain) {
        return getPermissions(domain.getCodeSource());
      }

      @Override
      public boolean implies(final ProtectionDomain domain, final Permission permission) {
        return true;

      }
    });
  }

  public synchronized void pushInUdf() {
    if (validateCaller()) {
      if (UDF_IS_EXECUTING.get() == null) {
        UDF_IS_EXECUTING.set(new Stack<>());
      }
      UDF_IS_EXECUTING.get().push(true);
    }
  }

  public void popOutUdf() {
    if (validateCaller()) {
      final Stack<Boolean> stack = UDF_IS_EXECUTING.get();
      if (stack != null && !stack.isEmpty()) {
        stack.pop();
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

  @Override
  public void checkExec(final String cmd) {
    if (inUdfExecution()) {
      throw new SecurityException("A UDF attempted to execute the following cmd: " + cmd);
    }
    super.checkExec(cmd);
  }


  private boolean inUdfExecution() {
    final Stack<Boolean> executing = UDF_IS_EXECUTING.get();
    return executing != null && !executing.isEmpty();
  }

  /**
   * Check if the caller is a PluggableUdf. It will be the third
   * item in the class array.
   * @return true if caller is allowed
   */
  private boolean validateCaller() {
    return getClassContext()[2].equals(PluggableUdf.class);
  }
}
