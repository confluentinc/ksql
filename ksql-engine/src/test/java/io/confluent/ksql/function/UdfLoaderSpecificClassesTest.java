package io.confluent.ksql.function;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.util.KsqlException;
import java.io.File;
import java.util.Optional;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

public class UdfLoaderSpecificClassesTest {

  private static final ClassLoader PARENT_CLASS_LOADER = io.confluent.ksql.function.UdfLoaderSpecificClassesTest.class.getClassLoader();
  private static final UdfCompiler COMPILER = new UdfCompiler(Optional.empty());

  @Test
  public void shouldNotLoadInternalUdfs() {
    final MutableFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    final UdfLoader udfLoader = new UdfLoader(functionRegistry,
                                              new File("src/test/resources"),
                                              PARENT_CLASS_LOADER,
                                              value -> false,
                                              COMPILER,
                                              Optional.empty(),
                                              false);
    udfLoader.loadUdfFromClass(SomeFunctionUdf.class);
    try {
      functionRegistry.getUdfFactory("substring");
      fail("Should have thrown as function should not be loaded.");
    } catch (final KsqlException e) {
      // pass
    }
  }

  @Test
  public void shouldLoadSomeFunction() {
    final MutableFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    final UdfLoader udfLoader = new UdfLoader(functionRegistry,
                                              new File("src/test/resources"),
                                              PARENT_CLASS_LOADER,
                                              value -> false,
                                              COMPILER,
                                              Optional.empty(),
                                              false);
    udfLoader.loadUdfFromClass(SomeFunctionUdf.class);

    final UdfFactory udfFactory = functionRegistry.getUdfFactory("somefunction");
    assertThat(udfFactory, not(nullValue()));

    final KsqlFunction function = udfFactory.getFunction(ImmutableList.of(
        Schema.OPTIONAL_STRING_SCHEMA));
    assertThat(function.getFunctionName(),equalToIgnoringCase("somefunction"));

  }

  @SuppressWarnings({"unused", "MethodMayBeStatic"}) // Invoked via reflection in test.
  @UdfDescription(
      name = "SomeFunction",
      description = "A test-only UDF for testing")
  public static class SomeFunctionUdf {
    @Udf
    public int foo(final String value) {
      return 0;
    }
  }


}
