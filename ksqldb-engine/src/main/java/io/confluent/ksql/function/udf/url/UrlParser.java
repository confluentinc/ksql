/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.function.udf.url;

import io.confluent.ksql.function.KsqlFunctionException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * Utility class for extracting information from a String encoded URL
 */
final class UrlParser {

  private UrlParser() {

  }

  /**
   * @param url       an application/x-www-form-urlencoded string
   * @param extract   a method that accepts a {@link URI} and returns the value to extract
   *
   * @return the value of {@code extract(url)} if present and valid, otherwise {@code null}
   */
  static <T> T extract(final String url, @Nonnull final Function<URI, T> extract) {
    Objects.requireNonNull(extract, "must supply a non-null extract method!");

    if (url == null) {
      return null;
    }

    try {
      return extract.apply(new URI(url));
    } catch (final URISyntaxException e) {
      throw new KsqlFunctionException("URL input has invalid syntax: " + url, e);
    }
  }
}
