package io.confluent.ksql.function.udf.math;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;

@UdfDescription(
        name = "asin",
        category = FunctionCategory.MATHEMATICAL,
        author = KsqlConstants.CONFLUENT_AUTHOR,
        description = "The inverse (arc) sine of a value. The returned value is in radians."
)
public class Asin {

    @Udf(description = "Returns the inverse (arc) sine of an INT value")
    public Double asin(
            @UdfParameter(
                    value = "value",
                    description = "The value to get the inverse sine of."
            ) final Integer value
    ) {
        return asin(value == null ? null : value.doubleValue());
    }

    @Udf(description = "Returns the inverse (arc) sine of a BIGINT value")
    public Double asin(
            @UdfParameter(
                    value = "value",
                    description = "The value to get the inverse sine of."
            ) final Long value
    ) {
        return asin(value == null ? null : value.doubleValue());
    }

    @Udf(description = "Returns the inverse (arc) sine of a DOUBLE value")
    public Double asin(
            @UdfParameter(
                    value = "value",
                    description = "The value to get the inverse sine of."
            ) final Double value
    ) {
        return value == null
                ? null
                : Math.asin(value);
    }
}