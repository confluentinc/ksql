package io.confluent.ksql.function.udf.math;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;

@UdfDescription(
        name = "cos",
        category = FunctionCategory.MATHEMATICAL,
        author = KsqlConstants.CONFLUENT_AUTHOR,
        description = "The cosine of a value."
)
public class Cos {

    @Udf(description = "Returns the cosine of an INT value")
    public Double cos(
            @UdfParameter(
                    value = "value",
                    description = "The value in radians to get the cosine of."
            ) final Integer value
    ) {
        return cos(value == null ? null : value.doubleValue());
    }

    @Udf(description = "Returns the cosine of a BIGINT value")
    public Double cos(
            @UdfParameter(
                    value = "value",
                    description = "The value in radians to get the cosine of."
            ) final Long value
    ) {
        return cos(value == null ? null : value.doubleValue());
    }

    @Udf(description = "Returns the cosine of a DOUBLE value")
    public Double cos(
            @UdfParameter(
                    value = "value",
                    description = "The value in radians to get the cosine of."
            ) final Double value
    ) {
        return value == null
                ? null
                : Math.cos(value);
    }
}
