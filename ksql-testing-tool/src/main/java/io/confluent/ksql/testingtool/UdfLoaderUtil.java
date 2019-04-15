package io.confluent.ksql.testingtool;

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlFunction;
import io.confluent.ksql.function.MutableFunctionRegistry;
import io.confluent.ksql.function.UdfCompiler;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.UdfLoader;
import io.confluent.ksql.function.udf.UdfMetadata;
import java.util.Optional;
import org.apache.kafka.test.TestUtils;

public final class UdfLoaderUtil {
  private UdfLoaderUtil() {}

  public static FunctionRegistry load(final MutableFunctionRegistry functionRegistry) {
    new UdfLoader(functionRegistry,
        TestUtils.tempDirectory(),
        UdfLoaderUtil.class.getClassLoader(),
        value -> false, new UdfCompiler(Optional.empty()), Optional.empty(), true)
        .load();

    return functionRegistry;
  }

  public static UdfFactory createTestUdfFactory(final KsqlFunction udf) {
    final UdfMetadata metadata = new UdfMetadata(
        udf.getFunctionName(),
        udf.getDescription(),
        "Test Author",
        "",
        "internal",
        false);

    return new UdfFactory(udf.getKudfClass(), metadata);
  }
}
