package io.confluent.ksql.function.udf.math;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.util.KsqlConstants;

@UdfDescription(
        name = "pi",
        category = FunctionCategory.MATHEMATICAL,
        author = KsqlConstants.CONFLUENT_AUTHOR,
        description = "Returns the approximate value of pi."
)
public class Pi {
    @Udf(description = "Returns the approximate value of pi")
    public Double pi() {
        return Math.PI;
    }
}
