package io.confluent.ksql.function.udf.math;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;

@UdfDescription(
        name = "acos",
        category = FunctionCategory.MATHEMATICAL,
        author = KsqlConstants.CONFLUENT_AUTHOR,
        description = "The inverse (arc) cosine of a value. The returned value is in radians."
)
public class Acos {

    @Udf(description = "Returns the inverse (arc) cosine of an INT value")
    public Double acos(
            @UdfParameter(
                    value = "value",
                    description = "The value to get the inverse cosine of."
            ) final Integer value
    ) {
        return acos(value == null ? null : value.doubleValue());
    }

    @Udf(description = "Returns the inverse (arc) cosine of a BIGINT value")
    public Double acos(
            @UdfParameter(
                    value = "value",
                    description = "The value to get the inverse cosine of."
            ) final Long value
    ) {
        return acos(value == null ? null : value.doubleValue());
    }

    @Udf(description = "Returns the inverse (arc) cosine of a DOUBLE value")
    public Double acos(
            @UdfParameter(
                    value = "value",
                    description = "The value to get the inverse cosine of."
            ) final Double value
    ) {
        return value == null
                ? null
                : Math.acos(value);
    }
}
