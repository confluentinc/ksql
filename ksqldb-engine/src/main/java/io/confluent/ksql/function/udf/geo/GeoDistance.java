/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udf.geo;

import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.GrammaticalJoiner;
import io.confluent.ksql.util.KsqlConstants;
import java.util.List;

/**
 * Compute the distance between two points on the surface of the earth, according to the Haversine
 * formula for "great circle distance". The 2 input points should be specified as (lat, lon) pairs,
 * measured in decimal degrees.
 *
 * <p>An optional fifth parameter allows to specify either "MI" (miles) or "KM" (kilometers) as the
 * desired unit for the output measurement. Default is KM.
 */
@UdfDescription(
    name = "geo_distance",
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Compute the distance between two points on the surface of the earth,"
        + " according to the Haversine formula for \"great circle distance\"."
)
public class GeoDistance {

  // effective value of Earth radius (note we technically live on a slightly squashed sphere, not
  // a truly round one, so different authorities will quote slightly different values for the 'best'
  // value to use as effective radius). The difference between the 2 most commonly-quoted values
  // measures out to about 0.1% in most real-world cases, which is within the margin of error of
  // using this kind of great-circle methodology anyway (~0.5%).
  private static final double EARTH_RADIUS_KM = 6371;
  private static final double EARTH_RADIUS_MILES = 3959;

  private static final List<String> VALID_RADIUS_NAMES_MILES =
      Lists.newArrayList("mi", "mile", "miles");
  private static final List<String> VALID_RADIUS_NAMES_KMS =
      Lists.newArrayList("km", "kilometer", "kilometers", "kilometre", "kilometres");

  @SuppressWarnings("UnstableApiUsage")
  private static final String VALID_VALUES = "one of "
      + GrammaticalJoiner.or().join(Streams.concat(
      VALID_RADIUS_NAMES_MILES.stream(),
      VALID_RADIUS_NAMES_KMS.stream()
  ));

  @SuppressWarnings("MethodMayBeStatic")
  @Udf(description = "The 2 input points should be specified as (lat, lon) pairs, measured"
      + " in decimal degrees. An optional fifth parameter allows to specify either \"MI\" (miles)"
      + " or \"KM\" (kilometers) as the desired unit for the output measurement. Default is KM.")
  public Double geoDistance(
      @UdfParameter(description = "The latitude of the first point in decimal degrees.")
        final double lat1,
      @UdfParameter(description = "The longitude of the first point in decimal degrees.")
        final double lon1,
      @UdfParameter(description = "The latitude of the second point in decimal degrees.")
        final double lat2,
      @UdfParameter(description = "The longitude of the second point in decimal degrees.")
        final double lon2,
      @UdfParameter(description = "The units for the return value. Either MILES or KM.")
        final String units
  ) {
    validateLatLonValues(lat1, lon1, lat2, lon2);
    final double chosenRadius = selectEarthRadiusToUse(units);

    final double deltaLat = Math.toRadians(lat2 - lat1);
    final double deltaLon = Math.toRadians(lon2 - lon1);

    final double lat1Radians = Math.toRadians(lat1);
    final double lat2Radians = Math.toRadians(lat2);

    final double a =
        haversin(deltaLat) + haversin(deltaLon) * Math.cos(lat1Radians) * Math.cos(lat2Radians);
    final double distanceInRadians = 2 * Math.asin(Math.sqrt(a));
    return distanceInRadians * chosenRadius;
  }

  @Udf(description = "The 2 input points should be specified as (lat, lon) pairs, measured"
      + " in decimal degrees. The distance returned is in kilometers.")
  public Double geoDistance(
      @UdfParameter(description = "The latitude of the first point in decimal degrees.")
      final double lat1,
      @UdfParameter(description = "The longitude of the first point in decimal degrees.")
      final double lon1,
      @UdfParameter(description = "The latitude of the second point in decimal degrees.")
      final double lat2,
      @UdfParameter(description = "The longitude of the second point in decimal degrees.")
      final double lon2) {

    return geoDistance(lat1, lon1, lat2, lon2, VALID_RADIUS_NAMES_KMS.get(0));
  }

  private static void validateLatLonValues(
      final double lat1, final double lon1, final double lat2, final double lon2) {
    if (lat1 < -90 || lat2 < -90 || lat1 > 90 || lat2 > 90) {
      throw new KsqlFunctionException(
          "valid latitude values for GeoDistance function are in the range of -90 to 90"
              + " decimal degrees");
    }
    if (lon1 < -180 || lon2 < -180 || lon1 > 180 || lon2 > 180) {
      throw new KsqlFunctionException(
          "valid longitude values for GeoDistance function are in the range of -180 to +180"
              + " decimal degrees");
    }
  }

  private static double selectEarthRadiusToUse(final String units) {
    double chosenRadius = EARTH_RADIUS_KM;
    if (units != null && units.trim().length() > 0) {
      final String outputUnit = units.trim().toLowerCase();
      if (VALID_RADIUS_NAMES_MILES.contains(outputUnit)) {
        chosenRadius = EARTH_RADIUS_MILES;
      } else if (VALID_RADIUS_NAMES_KMS.contains(outputUnit)) {
        chosenRadius = EARTH_RADIUS_KM;
      } else {
        throw new KsqlFunctionException(
            "GeoDistance function units parameter must be "
                + VALID_VALUES
                + ". Values are case-insensitive.");
      }
    }
    return chosenRadius;
  }

  private static double haversin(final double val) {
    return Math.pow(Math.sin(val / 2), 2);
  }
}
