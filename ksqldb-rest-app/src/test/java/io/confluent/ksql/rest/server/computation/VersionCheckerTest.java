/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.rest.server.computation;

import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import io.confluent.ksql.rest.server.computation.Command.VersionChecker;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Version;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.maven.artifact.versioning.ArtifactVersion;
import org.apache.maven.artifact.versioning.DefaultArtifactVersion;
import org.junit.Test;

public class VersionCheckerTest {

  @Test
  public void ensureCompatMatrixContainsCurrentVersion() {
    // When (does not throw):
    new VersionChecker(Version::getVersion).throwOnIncompatibleCommandVersion(Version.getVersion());
  }

  @Test
  public void ensureThrowsOnFutureServerVersion() {
    // Given:
    final ArtifactVersion current = new DefaultArtifactVersion(Version.getVersion());
    final String next = Stream.of(
        current.getMajorVersion(),
        current.getMinorVersion(),
        current.getIncrementalVersion() + 1
    ).map(Object::toString).collect(Collectors.joining("."));

    // When:
    assertThrows(
        IllegalStateException.class,
        () -> new VersionChecker(() -> next).throwOnIncompatibleCommandVersion("0.0"));
  }

  @Test
  public void shouldThrowOnIncompatibleCommand() {
    // Given:
    final String commandVersion = "0.1.0";
    final RangeMap<ArtifactVersion, Range<ArtifactVersion>> compatMatrix = ImmutableRangeMap
        .of(Range.singleton(new DefaultArtifactVersion(Version.getVersion())),
            Range.singleton(new DefaultArtifactVersion("0.2.0")));

    // When:
    assertThrows(
        KsqlException.class,
        () -> new VersionChecker(compatMatrix, Version::getVersion).throwOnIncompatibleCommandVersion(commandVersion)
    );
  }


}