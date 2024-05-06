/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.rest.entity;

import io.confluent.ksql.util.KsqlException;
import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Ksql media types that can be used to in {@code content-type} and {@code accepts} HTTP headers.
 */
public enum KsqlMediaType {

  KSQL_V1_JSON("application/vnd.ksql.v1+json"),
  KSQL_V1_PROTOBUF("application/vnd.ksql.v1+protobuf");

  public static final KsqlMediaType LATEST_FORMAT = KSQL_V1_JSON;
  public static final KsqlMediaType LATEST_FORMAT_PROTOBUF = KSQL_V1_PROTOBUF;

  private final int version;
  private final String mediaType;

  KsqlMediaType(final String mediaType) {
    this.mediaType = Objects.requireNonNull(mediaType, "mediaType");

    final Matcher matcher = Pattern.compile("application/.+\\.v(?<version>\\d+)[^0-9]+.*")
        .matcher(mediaType);

    if (!matcher.matches()) {
      throw new IllegalStateException("Invalid mediaType: " + mediaType);
    }

    this.version = Integer.parseInt(matcher.group("version"));
  }

  public int getVersion() {
    return version;
  }

  public String mediaType() {
    return mediaType;
  }

  public static KsqlMediaType parse(final String mediaType) {
    return Arrays.stream(values())
        .filter(mt -> mt.mediaType.equals(mediaType))
        .findFirst()
        .orElseThrow(() -> new KsqlException("Unsupported media type: " + mediaType));
  }

  public static KsqlMediaType valueOf(final String format, final int version) {
    final String name = "KSQL_V" + version + "_" + format.toUpperCase();
    return valueOf(name);
  }
}
