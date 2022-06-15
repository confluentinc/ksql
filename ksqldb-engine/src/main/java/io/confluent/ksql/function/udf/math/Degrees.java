package io.confluent.ksql.function.udf.math;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;

@UdfDescription(
        name = "degrees",
        category = FunctionCategory.MATHEMATICAL,
        author = KsqlConstants.CONFLUENT_AUTHOR,
        description = "Converts a value in radians to a value in degrees."
)
public class Degrees {

    @Udf(description = "Converts an INT value in radians to a value in degrees")
    public Double degrees(
            @UdfParameter(
                    value = "value",
                    description = "The value in radians to convert to degrees."
            ) final Integer value
    ) {
        return degrees(value == null ? null : value.doubleValue());
    }

    @Udf(description = "Converts a BIGINT value in radians to a value in degrees")
    public Double degrees(
            @UdfParameter(
                    value = "value",
                    description = "The value in radians to convert to degrees."
            ) final Long value
    ) {
        return degrees(value == null ? null : value.doubleValue());
    }

    @Udf(description = "Converts a DOUBLE value in radians to a value in degrees")
    public Double degrees(
            @UdfParameter(
                    value = "value",
                    description = "The value in radians to convert to degrees."
            ) final Double value
    ) {
        return value == null
                ? null
                : Math.toDegrees(value);
    }
}
