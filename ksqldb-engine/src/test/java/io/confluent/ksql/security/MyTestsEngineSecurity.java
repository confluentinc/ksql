package io.confluent.ksql.security;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        DefaultKsqlPrincipalTest.class,
//        Bazel doesn't support custom security managers, so these need to be re-worked.
//        ExtensionSecurityManagerTest.class,
        KsqlAuthorizationValidatorFactoryTest.class,
        KsqlAuthorizationValidatorImplTest.class,
        KsqlBackendAccessValidatorTest.class,
        KsqlCacheAccessValidatorTest.class,
        KsqlProvidedAccessValidatorTest.class,
})
public class MyTestsEngineSecurity {
}
