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

package io.confluent.ksql.rest.server.filters;

import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.server.resources.HealthCheckResource;
import io.confluent.ksql.rest.server.resources.ServerMetadataResource;
import io.confluent.ksql.security.KsqlAuthorizationProvider;
import java.lang.reflect.Method;
import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Priority;
import javax.ws.rs.Path;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authorization filter for REST endpoints.
 */
@Priority(Priorities.AUTHORIZATION)
public class KsqlAuthorizationFilter implements ContainerRequestFilter  {
  private static final Logger log = LoggerFactory.getLogger(KsqlAuthorizationFilter.class);

  private static final Set<String> PATHS_WITHOUT_AUTHORIZATION = getPathsFrom(
      ServerMetadataResource.class,
      HealthCheckResource.class
  );

  private final KsqlAuthorizationProvider authorizationProvider;

  public KsqlAuthorizationFilter(final KsqlAuthorizationProvider authorizationProvider) {
    this.authorizationProvider = authorizationProvider;
  }

  @Override
  public void filter(final ContainerRequestContext requestContext) {
    final Principal user = requestContext.getSecurityContext().getUserPrincipal();
    final String method = requestContext.getMethod(); // i.e GET, POST
    final String path = "/" + requestContext.getUriInfo().getPath();

    if (!requiresAuthorization(path)) {
      return;
    }

    try {
      authorizationProvider.checkEndpointAccess(user, method, path);
    } catch (final Throwable t) {
      log.warn(String.format("User:%s is denied access to \"%s %s\"",
          user.getName(), method, path), t);
      requestContext.abortWith(Errors.accessDenied(t.getMessage()));
    }
  }

  public static Set<String> getPathsWithoutAuthorization() {
    return PATHS_WITHOUT_AUTHORIZATION;
  }

  private boolean requiresAuthorization(final String path) {
    return !PATHS_WITHOUT_AUTHORIZATION.contains(path);
  }

  private static Set<String> getPathsFrom(final Class<?> ...resourceClass) {

    final Set<String> paths = new HashSet<>();
    for (final Class<?> clazz : resourceClass) {
      final String mainPath = StringUtils.stripEnd(
          clazz.getAnnotation(Path.class).value(), "/"
      );

      paths.add(mainPath);
      for (final Method m : clazz.getMethods()) {
        if (m.isAnnotationPresent(Path.class)) {
          paths.add(mainPath + "/"
              + StringUtils.strip(m.getAnnotation(Path.class).value(), "/"));
        }
      }
    }

    return Collections.unmodifiableSet(paths);
  }
}
