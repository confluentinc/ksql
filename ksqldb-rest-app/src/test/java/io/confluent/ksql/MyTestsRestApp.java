package io.confluent.ksql;

import io.confluent.ksql.api.auth.AuthenticationPluginHandlerTest;
import io.confluent.ksql.api.auth.BasicCallbackHandlerTest;
import io.confluent.ksql.api.auth.JaasAuthProviderTest;
import io.confluent.ksql.api.auth.KsqlAuthorizationProviderHandlerTest;
import io.confluent.ksql.api.auth.SystemAuthenticationHandlerTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        AuthenticationPluginHandlerTest.class,
        BasicCallbackHandlerTest.class,
        JaasAuthProviderTest.class,
        KsqlAuthorizationProviderHandlerTest.class,
        SystemAuthenticationHandlerTest.class
        // todo: add more tests
})
public class MyTestsRestApp {
}
