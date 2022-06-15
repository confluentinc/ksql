package io.confluent.ksql.function.udf.math;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;

@UdfDescription(
        name = "atan2",
        category = FunctionCategory.MATHEMATICAL,
        author = KsqlConstants.CONFLUENT_AUTHOR,
        description = "The inverse (arc) tangent of y / x. This is equivalent to the angle theta when Cartesian " +
                "coordinates (x, y) is converted to polar coordinates (radius, theta). The returned value is in " +
                "radians."
)
public class Atan2 {

    @Udf(description = "Returns the inverse (arc) tangent of y / x")
    public Double atan2(
            @UdfParameter(
                    value = "y",
                    description = "The ordinate (y) coordinate."
            ) final Integer y,
            @UdfParameter(
                    value = "x",
                    description = "The abscissa (x) coordinate."
            ) final Integer x
    ) {
        return atan2(y == null ? null : y.doubleValue(), x == null ? null : x.doubleValue());
    }

    @Udf(description = "Returns the inverse (arc) tangent of y / x")
    public Double atan2(
            @UdfParameter(
                    value = "y",
                    description = "The ordinate (y) coordinate."
            ) final Integer y,
            @UdfParameter(
                    value = "x",
                    description = "The abscissa (x) coordinate."
            ) final Long x
    ) {
        return atan2(y == null ? null : y.doubleValue(), x == null ? null : x.doubleValue());
    }

    @Udf(description = "Returns the inverse (arc) tangent of y / x")
    public Double atan2(
            @UdfParameter(
                    value = "y",
                    description = "The ordinate (y) coordinate."
            ) final Integer y,
            @UdfParameter(
                    value = "x",
                    description = "The abscissa (x) coordinate."
            ) final Double x
    ) {
        return atan2(y == null ? null : y.doubleValue(), x);
    }

    @Udf(description = "Returns the inverse (arc) tangent of y / x")
    public Double atan2(
            @UdfParameter(
                    value = "y",
                    description = "The ordinate (y) coordinate."
            ) final Long y,
            @UdfParameter(
                    value = "x",
                    description = "The abscissa (x) coordinate."
            ) final Integer x
    ) {
        return atan2(y == null ? null : y.doubleValue(), x == null ? null : x.doubleValue());
    }

    @Udf(description = "Returns the inverse (arc) tangent of y / x")
    public Double atan2(
            @UdfParameter(
                    value = "y",
                    description = "The ordinate (y) coordinate."
            ) final Long y,
            @UdfParameter(
                    value = "x",
                    description = "The abscissa (x) coordinate."
            ) final Long x
    ) {
        return atan2(y == null ? null : y.doubleValue(), x == null ? null : x.doubleValue());
    }

    @Udf(description = "Returns the inverse (arc) tangent of y / x")
    public Double atan2(
            @UdfParameter(
                    value = "y",
                    description = "The ordinate (y) coordinate."
            ) final Long y,
            @UdfParameter(
                    value = "x",
                    description = "The abscissa (x) coordinate."
            ) final Double x
    ) {
        return atan2(y == null ? null : y.doubleValue(), x);
    }

    @Udf(description = "Returns the inverse (arc) tangent of y / x")
    public Double atan2(
            @UdfParameter(
                    value = "y",
                    description = "The ordinate (y) coordinate."
            ) final Double y,
            @UdfParameter(
                    value = "x",
                    description = "The abscissa (x) coordinate."
            ) final Integer x
    ) {
        return atan2(y, x == null ? null : x.doubleValue());
    }

    @Udf(description = "Returns the inverse (arc) tangent of y / x")
    public Double atan2(
            @UdfParameter(
                    value = "y",
                    description = "The ordinate (y) coordinate."
            ) final Double y,
            @UdfParameter(
                    value = "x",
                    description = "The abscissa (x) coordinate."
            ) final Long x
    ) {
        return atan2(y, x == null ? null : x.doubleValue());
    }

    @Udf(description = "Returns the inverse (arc) tangent of y / x")
    public Double atan2(
            @UdfParameter(
                    value = "y",
                    description = "The ordinate (y) coordinate."
            ) final Double y,
            @UdfParameter(
                    value = "x",
                    description = "The abscissa (x) coordinate."
            ) final Double x
    ) {
        return x == null || y == null
                ? null
                : Math.atan2(y, x);
    }
}
