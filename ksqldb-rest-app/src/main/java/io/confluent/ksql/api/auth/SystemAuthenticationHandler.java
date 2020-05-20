package io.confluent.ksql.api.auth;

import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

import io.confluent.ksql.api.server.ErrorCodes;
import io.confluent.ksql.api.server.InternalEndpointHandler;
import io.confluent.ksql.api.server.KsqlApiException;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import java.net.URI;
import java.security.Principal;
import java.util.Objects;

public class SystemAuthenticationHandler implements Handler<RoutingContext> {

  private final KsqlRestConfig ksqlRestConfig;

  public SystemAuthenticationHandler(
      final KsqlRestConfig ksqlRestConfig,
      boolean fromInternalListener,
      final URI internalListener
  ) {
    this.ksqlRestConfig = ksqlRestConfig;
  }

  @Override
  public void handle(RoutingContext routingContext) {
    // The requirements for being considered a system call on behalf of the SystemUser are that
    // SSL mutual auth is in effect for the connection (meaning that the request is verified to be
    // coming from a known set of servers in the cluster), and that it came on the internal
    // listener interface, meaning that it's being done with the authorization of the system
    // rather than directly on behalf of the user. Mutual auth is only enforced when SSL is used.
//    boolean isFromInternalListener = routingContext.get(InternalEndpointHandler.)
//    if ((ksqlRestConfig.getClientAuthInternal() != ClientAuth.REQUIRED &&
//        internalListener.getScheme().equals("https")) ||
//        !fromInternalListener) {
//      routingContext
//          .fail(UNAUTHORIZED.code(),
//              new KsqlApiException("Unauthorized", ErrorCodes.ERROR_FAILED_AUTHENTICATION));
//      return;
//    }

    routingContext.setUser(new SystemUser(new SystemPrincipal()));
    routingContext.next();
  }

  public static boolean isAuthenticatedAsSystemUser(RoutingContext routingContext) {
    User user = routingContext.user();
    return user instanceof SystemUser;
  }

  private static class SystemPrincipal implements Principal {
    private static final String SYSTEM_NAME = "__system__";

    SystemPrincipal() {}

    @Override
    public String getName() {
      return SYSTEM_NAME;
    }
  }

  private static class SystemUser implements ApiUser {

    private final Principal principal;

    SystemUser(final Principal principal) {
      this.principal = Objects.requireNonNull(principal);
    }

    @SuppressWarnings("deprecation")
    @Override
    public User isAuthorized(final String s, final Handler<AsyncResult<Boolean>> handler) {
      throw new UnsupportedOperationException();
    }

    @SuppressWarnings("deprecation")
    @Override
    public User clearCache() {
      throw new UnsupportedOperationException();
    }

    @Override
    public JsonObject principal() {
      throw new UnsupportedOperationException();
    }

    @SuppressWarnings("deprecation")
    @Override
    public void setAuthProvider(final AuthProvider authProvider) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Principal getPrincipal() {
      return principal;
    }
  }
}
