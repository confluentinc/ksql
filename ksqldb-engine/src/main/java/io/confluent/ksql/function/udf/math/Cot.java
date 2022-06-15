package io.confluent.ksql.function.udf.math;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;

@UdfDescription(
        name = "cot",
        category = FunctionCategory.MATHEMATICAL,
        author = KsqlConstants.CONFLUENT_AUTHOR,
        description = "The cotangent of a value."
)
public class Cot {

    @Udf(description = "Returns the cotangent of an INT value")
    public Double cot(
            @UdfParameter(
                    value = "value",
                    description = "The value in radians to get the cotangent of."
            ) final Integer value
    ) {
        return cot(value == null ? null : value.doubleValue());
    }

    @Udf(description = "Returns the cotangent of a BIGINT value")
    public Double cot(
            @UdfParameter(
                    value = "value",
                    description = "The value in radians to get the cotangent of."
            ) final Long value
    ) {
        return cot(value == null ? null : value.doubleValue());
    }

    @Udf(description = "Returns the cotangent of a DOUBLE value")
    public Double cot(
            @UdfParameter(
                    value = "value",
                    description = "The value in radians to get the cotangent of."
            ) final Double value
    ) {
        return value == null
                ? null
                : 1 / Math.tan(value);
    }
}