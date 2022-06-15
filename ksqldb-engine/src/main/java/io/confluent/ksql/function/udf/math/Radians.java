package io.confluent.ksql.function.udf.math;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;

@UdfDescription(
        name = "radians",
        category = FunctionCategory.MATHEMATICAL,
        author = KsqlConstants.CONFLUENT_AUTHOR,
        description = "Converts a value in degrees to a value in radians."
)
public class Radians {

    @Udf(description = "Converts an INT value in degrees to a value in radians")
    public Double radians(
            @UdfParameter(
                    value = "value",
                    description = "The value in degrees to convert to radians."
            ) final Integer value
    ) {
        return radians(value == null ? null : value.doubleValue());
    }

    @Udf(description = "Converts a BIGINT value in degrees to a value in radians")
    public Double radians(
            @UdfParameter(
                    value = "value",
                    description = "The value in degrees to convert to radians."
            ) final Long value
    ) {
        return radians(value == null ? null : value.doubleValue());
    }

    @Udf(description = "Converts a DOUBLE value in degrees to a value in radians")
    public Double radians(
            @UdfParameter(
                    value = "value",
                    description = "The value in degrees to convert to radians."
            ) final Double value
    ) {
        return value == null
                ? null
                : Math.toRadians(value);
    }
}
