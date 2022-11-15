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


package io.confluent.ksql.rest.server;

import static io.confluent.ksql.rest.server.LocalCommands.LOCAL_COMMANDS_PROCESSED_SUFFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.test.util.KsqlTestFolder;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

// some tests here make sure a mocked exception isn't thrown
@RunWith(MockitoJUnitRunner.Silent.class)
public class LocalCommandsTest {
  private static final String QUERY_APP_ID1 = "appId1";
  private static final String QUERY_APP_ID2 = "appId2";
  private static final String QUERY_APP_ID3 = "appId3";

  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private TransientQueryMetadata metadata1;
  @Mock
  private TransientQueryMetadata metadata2;
  @Mock
  private TransientQueryMetadata metadata3;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private LocalCommandsFile localCommandsFile;

  @Rule
  public TemporaryFolder commandsDir = KsqlTestFolder.temporaryFolder();

  @Before
  public void setup() throws IOException {
    when(metadata1.getQueryApplicationId()).thenReturn(QUERY_APP_ID1);
    when(metadata2.getQueryApplicationId()).thenReturn(QUERY_APP_ID2);
    when(metadata3.getQueryApplicationId()).thenReturn(QUERY_APP_ID3);
  }

  @Test
  public void shouldThrowWhenCommandLocationIsNotDirectory() throws IOException {
    // Given
    File file = commandsDir.newFile();

    // When
    final Exception e = assertThrows(
        KsqlServerException.class,
        () -> LocalCommands.open(ksqlEngine, file)
    );

    // Then
    assertThat(e.getMessage(), containsString(String.format(
        "%s is not a directory.",
        file.getAbsolutePath()
    )));
  }

  @Test
  public void shouldThrowWhenCommandLocationIsNotWritable() throws IOException {
    // Given
    final File file = commandsDir.newFolder();
    Files.setPosixFilePermissions(file.toPath(), PosixFilePermissions.fromString("r-x------"));

    // When
    final Exception e = assertThrows(
        KsqlServerException.class,
        () -> LocalCommands.open(ksqlEngine, file)
    );

    // Then
    assertThat(e.getMessage(), containsString(String.format(
        "The local commands directory is not readable/writable for KSQL server: %s",
        file.getAbsolutePath()
    )));
  }

  @Test
  public void shouldThrowWhenCommandLocationIsNotReadable() throws IOException {
    // Given
    final File dir = commandsDir.newFolder();
    Files.setPosixFilePermissions(dir.toPath(), PosixFilePermissions.fromString("-wx------"));

    // When
    final Exception e = assertThrows(
        KsqlServerException.class,
        () -> LocalCommands.open(ksqlEngine, dir)
    );

    // Then
    assertThat(e.getMessage(), containsString(String.format(
        "The local commands directory is not readable/writable for KSQL server: %s",
        dir.getAbsolutePath()
    )));
  }


  @Test
  public void shouldCreateCommandLocationWhenDoesNotExist() throws IOException {
    // Given
    final Path dir = Paths.get(commandsDir.newFolder().getAbsolutePath(), "ksql-local-commands");
    assertThat(Files.exists(dir), is(false));

    // When
    LocalCommands.open(ksqlEngine, dir.toFile());

    // Then
    assertThat(Files.exists(dir), is(true));
  }

  @Test
  public void shouldWriteAppIdToCommandFile() throws IOException {
    // Given
    final File dir = commandsDir.newFolder();
    LocalCommands localCommands = LocalCommands.open(ksqlEngine, dir);
    File processedFile = localCommands.getCurrentLocalCommandsFile();

    // When
    localCommands.write(metadata1);
    localCommands.write(metadata2);

    // Then
    // Need to create a new local commands in order not to skip the "current" file we just wrote.
    localCommands = LocalCommands.open(ksqlEngine, dir);
    localCommands.write(metadata3);
    localCommands.processLocalCommandFiles(serviceContext);
    verify(ksqlEngine).cleanupOrphanedInternalTopics(any(),
        eq(ImmutableSet.of(QUERY_APP_ID1, QUERY_APP_ID2)));
    List<Path> paths = Files.list(dir.toPath()).collect(Collectors.toList());
    String expectedProcessedFileName = processedFile.getAbsolutePath()
        + LOCAL_COMMANDS_PROCESSED_SUFFIX;
    assertThat(paths.size(), is(2));
    assertThat(paths.stream().anyMatch(
        path -> path.toFile().getAbsolutePath().equals(expectedProcessedFileName)),
        is(true));
  }

  @Test
  public void shouldFailToWrite_error() throws IOException {
    // Given
    final File dir = commandsDir.newFolder();
    LocalCommands localCommands = new LocalCommands(dir, ksqlEngine, localCommandsFile);
    doThrow(new IOException("Error")).when(localCommandsFile).write(any());

    // When
    localCommands.write(metadata1);
  }

  @Test
  public void shouldNotThrowWhenFailToCleanup() throws IOException {
    // Given
    final File dir = commandsDir.newFolder();
    LocalCommands localCommands = LocalCommands.open(ksqlEngine, dir);
    doThrow(new KsqlServerException("Error")).when(localCommandsFile).readRecords();

    // When
    localCommands.write(metadata1);
    LocalCommands localCommands2 = LocalCommands.open(ksqlEngine, dir);
    localCommands2.write(metadata3);
    // Need to create a new local commands in order not to skip the "current" file we just wrote.
    localCommands2.processLocalCommandFiles(serviceContext);

    // Then no exception should be thrown
    verify(ksqlEngine).cleanupOrphanedInternalTopics(any(), eq(ImmutableSet.of(QUERY_APP_ID1)));
  }
}
