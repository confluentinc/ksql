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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.function.KsqlFunctionException;
import org.junit.Test;

public class GeoDistanceTest {
  private final GeoDistance distanceUdf = new GeoDistance();

  /*
   * Compute distance between Palo Alto and London Confluent offices.
   */
  @Test
  public void shouldComputeDistanceBetweenLocations() {
    assertEquals(8634.6528,
        (double) distanceUdf.geoDistance(37.4439, -122.1663, 51.5257, -0.1122), 0.5);
    assertEquals(8634.6528,
        (double) distanceUdf.geoDistance(37.4439, -122.1663, 51.5257, -0.1122, "KM"), 0.5);
    assertEquals(5365.66,
        (double) distanceUdf.geoDistance(37.4439, -122.1663, 51.5257, -0.1122, "MI"), 0.5);
  }

  /*
   * Compute distance between London and Cape Town
   */
  @Test
  public void shouldComputeDistanceDifferentHemisphere() {
    assertEquals(9673.4042,
        (double) distanceUdf.geoDistance(51.5257, -0.1122, -33.9323, 18.4197), 0.5);
    assertEquals(9673.4042,
        (double) distanceUdf.geoDistance(51.5257, -0.1122, -33.9323, 18.4197, "KM"), 0.5);
    assertEquals(6011.1453,
        (double) distanceUdf.geoDistance(51.5257, -0.1122, -33.9323, 18.4197, "MI"), 0.5);
  }

  /*
   * Compute distance between Cape Town and Sydney
   */
  @Test
  public void shouldComputeDistanceSouthHemisphere() {
    assertEquals(11005.2330,
        (double) distanceUdf.geoDistance(-33.9323, 18.4197, -33.8666, 151.1), 0.5);
    assertEquals(11005.2330,
        (double) distanceUdf.geoDistance(-33.9323, 18.4197, -33.8666, 151.1, "KM"), 0.5);
    assertEquals(6838.7564,
        (double) distanceUdf.geoDistance(-33.9323, 18.4197, -33.8666, 151.1, "MI"), 0.5);
  }

  /**
   * Valid values for latitude range from -90->90 decimal degrees, and longitude is from -180->180
   */
  @Test
  public void shouldFailOutOfBoundsCoordinates() {
    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> distanceUdf.geoDistance(90.1, -122.1663, -91.5257, -0.1122)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "valid latitude values for GeoDistance function are"));
  }

  @Test
  public void shouldFailInvalidUnitOfMeasure() {
    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> distanceUdf.geoDistance(37.4439, -122.1663, 51.5257, -0.1122, "Parsecs")
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "GeoDistance function units parameter must be one of"));
  }

  @Test
  public void shouldComputeDistanceBetweenLocationsWithNullUnitsUsingKM() {
    assertEquals(8634.6528,
        (double) distanceUdf.geoDistance(37.4439, -122.1663, 51.5257, -0.1122, null), 0.5);
  }
}
