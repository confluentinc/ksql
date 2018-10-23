package io.confluent.ksql.function.udf.geo;

import static org.junit.Assert.assertEquals;

import io.confluent.ksql.function.KsqlFunctionException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class GeoDistanceKudfTest {
  private GeoDistanceKudf distanceUdf = new GeoDistanceKudf();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  /*
   * Compute distance between Palo Alto and London Confluent offices.
   */
  @Test
  public void shouldComputeDistanceBetweenLocations() {
    assertEquals(8634.6528,
        (double) distanceUdf.evaluate(37.4439, -122.1663, 51.5257, -0.1122), 0.5);
    assertEquals(8634.6528,
        (double) distanceUdf.evaluate(37.4439, -122.1663, 51.5257, -0.1122, "KM"), 0.5);
    assertEquals(5365.66,
        (double) distanceUdf.evaluate(37.4439, -122.1663, 51.5257, -0.1122, "MI"), 0.5);
  }

  /*
   * Compute distance between London and Cape Town
   */
  @Test
  public void shouldComputeDistanceDifferentHemisphere() {
    assertEquals(9673.4042,
        (double) distanceUdf.evaluate(51.5257, -0.1122, -33.9323, 18.4197), 0.5);
    assertEquals(9673.4042,
        (double) distanceUdf.evaluate(51.5257, -0.1122, -33.9323, 18.4197, "KM"), 0.5);
    assertEquals(6011.1453,
        (double) distanceUdf.evaluate(51.5257, -0.1122, -33.9323, 18.4197, "MI"), 0.5);
  }

  /*
   * Compute distance between Cape Town and Sydney
   */
  @Test
  public void shouldComputeDistanceSouthHemisphere() {
    assertEquals(11005.2330,
        (double) distanceUdf.evaluate(-33.9323, 18.4197, -33.8666, 151.1), 0.5);
    assertEquals(11005.2330,
        (double) distanceUdf.evaluate(-33.9323, 18.4197, -33.8666, 151.1, "KM"), 0.5);
    assertEquals(6838.7564,
        (double) distanceUdf.evaluate(-33.9323, 18.4197, -33.8666, 151.1, "MI"), 0.5);
  }


  @Test
  public void shouldFailWithTooFewParams() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("GeoDistance function expects either 4 or 5 arguments");
    distanceUdf.evaluate(37.4439, -122.1663);
  }

  @Test
  public void shouldFailWithTooManyParams() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("GeoDistance function expects either 4 or 5 arguments");
    distanceUdf.evaluate(37.4439, -122.1663, 51.5257, -0.1122, "Foo", "Bar");
  }
  /**
   * Valid values for latitude range from -90->90 decimal degrees, and longitude is from -180->180
   */
  @Test
  public void shouldFailOutOfBoundsCoordinates() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("valid latitude values");
    distanceUdf.evaluate(90.1, -122.1663, -91.5257, -0.1122);
  }

  @Test
  public void shouldFailInvalidUnitOfMeasure() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("GeoDistance function fifth parameter must be");
    distanceUdf.evaluate(37.4439, -122.1663, 51.5257, -0.1122, "Widget");
  }
}
