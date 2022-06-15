package io.confluent.ksql.function.udf.math;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;

@UdfDescription(
        name = "tan",
        category = FunctionCategory.MATHEMATICAL,
        author = KsqlConstants.CONFLUENT_AUTHOR,
        description = "The tangent of a value."
)
public class Tan {

    @Udf(description = "Returns the tangent of an INT value")
    public Double tan(
            @UdfParameter(
                    value = "value",
                    description = "The value in radians to get the tangent of."
            ) final Integer value
    ) {
        return tan(value == null ? null : value.doubleValue());
    }

    @Udf(description = "Returns the tangent of a BIGINT value")
    public Double tan(
            @UdfParameter(
                    value = "value",
                    description = "The value in radians to get the tangent of."
            ) final Long value
    ) {
        return tan(value == null ? null : value.doubleValue());
    }

    @Udf(description = "Returns the tangent of a DOUBLE value")
    public Double tan(
            @UdfParameter(
                    value = "value",
                    description = "The value in radians to get the tangent of."
            ) final Double value
    ) {
        return value == null
                ? null
                : Math.tan(value);
    }
}