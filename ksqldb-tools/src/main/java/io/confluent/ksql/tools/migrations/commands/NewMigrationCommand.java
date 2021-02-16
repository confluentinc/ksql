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

package io.confluent.ksql.tools.migrations.commands;

import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.restrictions.Required;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(
    name = "new",
    description = "Creates a new migrations project, directory structure and config file."
)
public class NewMigrationCommand extends BaseCommand {

  private static final Logger LOGGER = LoggerFactory.getLogger(NewMigrationCommand.class);

  @Required
  @Arguments(description = "the project path to create the directory", title = "project-path")
  private String projectPath;

  @Override
  public void run() {
    final long startTime = System.currentTimeMillis();

    if (tryCreateDirectory(projectPath) &&
        tryCreateDirectory(projectPath + "/migrations") &&
        tryCreatePropertiesFile(projectPath + "/ksql-migrations.properties")) {
      final long endTime = System.currentTimeMillis();
      LOGGER.info("Migrations project directory created successfully (execution time " +
          (endTime - startTime)/1000.0 + "s)");
    } else {
      System.exit(1);
    }
  }

  private boolean tryCreateDirectory(final String path) {
    final File directory = new File(path);

    if (directory.exists() && directory.isDirectory()) {
      LOGGER.warn(path + " already exists. Skipping directory creation.");
      return true;
    } else if (directory.exists() && !directory.isDirectory()) {
      LOGGER.error(path + " already exists as a file. Cannot create directory.");
      return false;
    }

    try {
      LOGGER.info("Creating directory: " + path);
      Files.createDirectories(Paths.get(path));
    } catch(FileSystemException e) {
      LOGGER.error("Permission denied: create directory " + path);
      return false;
    } catch (IOException e) {
      LOGGER.error(String.format("Failed to create directory %s: %s", path, e.getMessage()));
      return false;
    }
    return true;
  }

  private boolean tryCreatePropertiesFile(final String path) {
    final File configFile = new File(path);
    if (configFile.exists()) {
      LOGGER.warn(path + " already exists. Skipping file creation.");
      return true;
    }
    try {
      LOGGER.info("Creating file: " + path);
      configFile.createNewFile();
    } catch (IOException e) {
      LOGGER.error(String.format("Failed to create file %s: %s", path, e.getMessage()));
      return false;
    }
    return true;
  }
}
