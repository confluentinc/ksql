/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KsqlVersion {

  private static final Pattern VERSION_PATTERN = Pattern.compile("v?([0-9]+)\\.([0-9]+)\\..*");
  private static final Map<KsqlVersion, KsqlVersion> versionMapping = new HashMap<>();

  private static final KsqlVersion CP_60 = new KsqlVersion("6.0.");
  private static final KsqlVersion CP_61 = new KsqlVersion("6.1.");

  private static final KsqlVersion V_010 = new KsqlVersion("0.10.");
  private static final KsqlVersion V_014 = new KsqlVersion("0.14.");

  static {
    versionMapping.put(CP_60, V_010);
    versionMapping.put(CP_61, V_014);
  }

  public enum VersionType {
    CONFLUENT_PLATFORM,
    KSQLDB_STANDALONE
  }

  private final VersionType versionType;
  private final String originalVersion;
  private final int majorVersion;
  private final int minorVersion;



  public KsqlVersion(final String version) {
    final Matcher matcher = VERSION_PATTERN.matcher(version);
    if (!matcher.find()) {
      throw new IllegalArgumentException("Unexpected ksqlDB server version: " + version);
    }

    originalVersion = version;
    majorVersion = Integer.parseInt(matcher.group(1));
    minorVersion = Integer.parseInt(matcher.group(2));
    versionType = majorVersion < 4
        ? VersionType.KSQLDB_STANDALONE
        : VersionType.CONFLUENT_PLATFORM;
  }

  /**
   * Returns {@code true} if this object is the same or a newer version than the provided version.
   * If this version and the provided version are of different type (ie, CP vs standalone),
   * we try to do a version mapping to make the comparison.
   *
   * <p>The comparison only take the major and minor version into account.
   *
   * <p><strong>If we cannot map the versions to each other,
   *            this methods returns {@code false}.</strong>
   *
   * @param version the base version to compare to
   * @return {@code false} if this version is older than the provided version,
   *         or if both versions cannot be compared; {@code true} otherwise
   */
  public boolean isAtLeast(final KsqlVersion version) {
    if (versionType == version.versionType) {
      return isAtLeastVersion(this, version.majorVersion, version.minorVersion);
    }

    if (versionType == VersionType.KSQLDB_STANDALONE) {
      final KsqlVersion otherStandalone = versionMapping.get(version);
      return otherStandalone != null
          && isAtLeastVersion(
              this,
              otherStandalone.majorVersion,
              otherStandalone.minorVersion
          );
    }

    final KsqlVersion standalone = versionMapping.get(this);
    return standalone != null
        && isAtLeastVersion(standalone, version.majorVersion, version.minorVersion);
  }

  private static boolean isAtLeastVersion(
      final KsqlVersion version,
      final int major,
      final int minor) {

    if (version.majorVersion > major) {
      return true;
    } else if (version.majorVersion < major) {
      return false;
    } else {
      return version.minorVersion >= minor;
    }
  }

  /**
   * Returns {@code true} if this object is the same version than the provided version.
   * If this version and the provided version are of different type (ie, CP vs standalone),
   * we try to do a version mapping to make the comparison.
   *
   * <p>The comparison only take the major and minor version into account.
   *
   * <p><strong>If we cannot map the versions to each other,
   *            this methods returns {@code false}.</strong>
   *
   * @param version the version to compare to
   * @return {@code false} if this version is not the same as the provided version,
   *         or if both versions cannot be compared; {@code true} otherwise
   */
  public boolean same(final KsqlVersion version) {
    return isAtLeast(version) && version.isAtLeast(this);
  }

  public VersionType type() {
    return versionType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(versionType, majorVersion, minorVersion);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KsqlVersion that = (KsqlVersion) o;
    return versionType == that.versionType
        && majorVersion == that.majorVersion
        && minorVersion == that.minorVersion;
  }

  @Override
  public String toString() {
    return originalVersion;
  }
}
