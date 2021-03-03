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

package io.confluent.ksql.tools.migrations.util;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public class MigrationVersionInfoFormatter {

  private static final List<VersionInfoField> VERSION_INFO_FIELDS = ImmutableList.of(
      new VersionInfoField("Version", vInfo -> String.valueOf(vInfo.getVersion())),
      new VersionInfoField("Name", MigrationVersionInfo::getName),
      new VersionInfoField("State", vInfo -> vInfo.getState().toString()),
      new VersionInfoField("Previous Version", MigrationVersionInfo::getPrevVersion),
      new VersionInfoField("Started On", MigrationVersionInfo::getStartedOn),
      new VersionInfoField("Completed On", MigrationVersionInfo::getCompletedOn),
      new VersionInfoField("Error Reason", MigrationVersionInfo::getErrorReason)
  );

  private final List<MigrationVersionInfo> versionInfos;

  public MigrationVersionInfoFormatter() {
    versionInfos = new ArrayList<>();
  }

  public void addVersionInfo(final MigrationVersionInfo versionInfo) {
    versionInfos.add(versionInfo);
  }

  public String getFormatted() {
    final List<Integer> columnLengths = VERSION_INFO_FIELDS.stream()
        .map(field -> {
          final int maxLength = Math.max(
              field.header.length(),
              versionInfos.stream()
                  .map(field.extractor)
                  .map(String::length)
                  .max(Integer::compare)
                  .orElse(0));
          return maxLength;
        })
        .collect(Collectors.toList());
    final String rowFormatString = constructRowFormatString(columnLengths);

    final StringBuilder builder = new StringBuilder();

    // format header
    builder.append(String.format(rowFormatString,
        VERSION_INFO_FIELDS.stream()
            .map(f -> f.header)
            .toArray()));

    // format divider
    final int totalColLength = columnLengths.stream()
        .reduce(Integer::sum)
        .orElseThrow(IllegalStateException::new);
    final int dividerLength = totalColLength + 3 * VERSION_INFO_FIELDS.size() - 1;
    final String divider = StringUtils.repeat("-", dividerLength);
    builder.append(divider);
    builder.append("\n");

    // format version info rows
    for (final MigrationVersionInfo result : versionInfos) {
      builder.append(String.format(rowFormatString,
          VERSION_INFO_FIELDS.stream()
              .map(f -> f.extractor.apply(result))
              .toArray()));
    }

    // format footer
    builder.append(divider);
    builder.append("\n");

    return builder.toString();
  }

  private static String constructRowFormatString(final List<Integer> lengths) {
    final List<String> columnFormatStrings = lengths.stream()
        .map(MigrationVersionInfoFormatter::constructSingleColumnFormatString)
        .collect(Collectors.toList());
    return String.format(" %s %n", String.join(" | ", columnFormatStrings));
  }

  private static String constructSingleColumnFormatString(final Integer length) {
    return String.format("%%%ds", (-1 * length));
  }

  private static class VersionInfoField {
    final String header;
    final Function<MigrationVersionInfo, String> extractor;

    VersionInfoField(
        final String header,
        final Function<MigrationVersionInfo, String> extractor
    ) {
      this.header = header;
      this.extractor = extractor;
    }
  }
}

