package io.confluent.ksql.function.udf.math;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;

@UdfDescription(
        name = "atan",
        category = FunctionCategory.MATHEMATICAL,
        author = KsqlConstants.CONFLUENT_AUTHOR,
        description = "The inverse (arc) tangent of a value. The returned value is in radians."
)
public class Atan {

    @Udf(description = "Returns the inverse (arc) tangent of an INT value")
    public Double atan(
            @UdfParameter(
                    value = "value",
                    description = "The value to get the inverse tangent of."
            ) final Integer value
    ) {
        return atan(value == null ? null : value.doubleValue());
    }

    @Udf(description = "Returns the inverse (arc) tangent of a BIGINT value")
    public Double atan(
            @UdfParameter(
                    value = "value",
                    description = "The value to get the inverse tangent of."
            ) final Long value
    ) {
        return atan(value == null ? null : value.doubleValue());
    }

    @Udf(description = "Returns the inverse (arc) tangent of a DOUBLE value")
    public Double atan(
            @UdfParameter(
                    value = "value",
                    description = "The value to get the inverse tangent of."
            ) final Double value
    ) {
        return value == null
                ? null
                : Math.atan(value);
    }
}