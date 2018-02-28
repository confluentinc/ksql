/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.ksql.function.udf.geo;

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Kudf;

/**
 * Compute the distance between two points on the surface of the earth, according to the Haversine
 * formula for "great circle distance". The 2 input points should be specified as (lat, lon) pairs,
 * measured in decimal degrees.
 * 
 * <p>An optional fifth parameter allows to specify either "MI" (miles) or "KM" (kilometers) as the
 * desired unit for the output measurement. Default is KM.
 *
 */
public class GeoDistance implements Kudf {

  // effective value of Earth radius (note we technically live on a slightly squashed sphere, not
  // a truly round one, so different authorities will quote slightly different values for the 'best'
  // value to use as effective radius. The difference between the 2 most commonly-quoted sets
  // measures out to about 0.1% in most real-world cases, which is within the margin of error of
  // using this kind of great-circle methodology anyway (~0.5%).
  private static final double EARTH_RADIUS_KM = 6371;
  private static final double EARTH_RADIUS_MILES = 3959;

  @Override
  public void init() {
    // wish it were possible to validate (at least some of) the input arguments here and cut down on
    // the redundant work.
  }

  @Override
  public Object evaluate(Object... args) {
    double chosenRadius = EARTH_RADIUS_KM;
    if ((args.length < 4) || (args.length > 5)) {
      throw new KsqlFunctionException(
          "GeoDistance function expects either 4 or 5 arguments: lat1, lon1, lat2, lon2, (MI/KM)");
    }
    if (args.length == 5) {
      String outputUnit = args[4].toString().toLowerCase();
      if (outputUnit.equals("mi") || outputUnit.startsWith("mile")) {
        chosenRadius = EARTH_RADIUS_MILES;
      } else if (outputUnit.equals("km") || outputUnit.startsWith("kilomet")) {
        chosenRadius = EARTH_RADIUS_KM;
      } else {
        throw new KsqlFunctionException(
            "GeoDistance function fifth parameter must be \"MI\" (miles) or \"KM\" (kilometers)");
      }
    }
    double lat1 = ((Number) args[0]).doubleValue();
    double lon1 = ((Number) args[1]).doubleValue();
    double lat2 = ((Number) args[2]).doubleValue();
    double lon2 = ((Number) args[3]).doubleValue();

    if (lat1 < 0 || lat2 < 0 || lat1 > 90 || lat2 > 90) {
      throw new KsqlFunctionException(
          "valid latitude values for GeoDistance function are in the range of 0 to 90"
              + " decimal degrees");
    }
    if (lon1 < -180 || lon2 < -180 || lon1 > 180 || lon2 > 180) {
      throw new KsqlFunctionException(
          "valid longitude values for GeoDistance function are in the range of -180 to +180"
              + " decimal degrees");
    }
    double deltaLat = Math.toRadians(lat2 - lat1);
    double deltaLon = Math.toRadians(lon2 - lon1);

    double lat1Radians = Math.toRadians(lat1);
    double lat2Radians = Math.toRadians(lat2);

    double a =
        haversin(deltaLat) + haversin(deltaLon) * Math.cos(lat1Radians) * Math.cos(lat2Radians);
    double distanceInRadians = 2 * Math.asin(Math.sqrt(a));
    return distanceInRadians * chosenRadius;
  }

  private static double haversin(double val) {
    return Math.pow(Math.sin(val / 2), 2);
  }
}
