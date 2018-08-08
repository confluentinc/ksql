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

  /**
   * First, the happy-path test: compute distance between Palo Alto and London offices.
   */
  @Test
  public void shouldComputeDistanceBetweenLocations() {
    assertEquals(8634.6528, (double) distanceUdf.evaluate(37.4439, -122.1663, 51.5257, -0.1122),
        0.5);
    assertEquals(8634.6528,
        (double) distanceUdf.evaluate(37.4439, -122.1663, 51.5257, -0.1122, "KM"), 0.5);
    assertEquals(5365.66, (double) distanceUdf.evaluate(37.4439, -122.1663, 51.5257, -0.1122, "MI"),
        0.5);
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
   * Valid values for latitude range from 0->90 decimal degrees, and longitude is from -180->180
   */
  @Test
  public void shouldFailOutOfBoundsCoordinates() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("valid latitude values");
    distanceUdf.evaluate(90.1, -122.1663, 51.5257, -0.1122);
  }

  @Test
  public void shouldFailInvalidUnitOfMeasure() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("GeoDistance function fifth parameter must be");
    distanceUdf.evaluate(37.4439, -122.1663, 51.5257, -0.1122, "Widget");
  }

}
