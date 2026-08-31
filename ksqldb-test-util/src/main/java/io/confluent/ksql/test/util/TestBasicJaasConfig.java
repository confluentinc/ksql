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

package io.confluent.ksql.test.util;

import static java.util.Objects.requireNonNull;

import com.google.common.base.Charsets;
import com.google.errorprone.annotations.Immutable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.security.auth.login.Configuration;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.rules.ExternalResource;

/**
 * Junit test resource that sets the Jaas config for the JVM
 */
public final class TestBasicJaasConfig extends ExternalResource {

  private final Path jaasFile;
  private Optional<String> previous = Optional.empty();

  private TestBasicJaasConfig(
      final Path jaasFile
  ) {
    this.jaasFile = requireNonNull(jaasFile, "jaasFile");
  }

  public static Builder builder(final String realm) {
    return new Builder(realm);
  }

  public Path jaasFile() {
    return jaasFile;
  }

  @Override
  protected void before() {
    previous = setJvmJaasConfig(Optional.of(jaasFile.toString()));
  }

  @Override
  protected void after() {
    setJvmJaasConfig(previous);
  }

  private static Optional<String> setJvmJaasConfig(final Optional<String> jaasPath) {
    final String previous = jaasPath.isPresent()
        ? System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jaasPath.get())
        : System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM);

    forceJvmToReloadJaasConfig();
    return Optional.ofNullable(previous);
  }

  private static void forceJvmToReloadJaasConfig() {
    Configuration.setConfiguration(null);
    Configuration.getConfiguration();
  }

  public static final class Builder {

    private final String realm;
    private final Map<String, UserEntry> users = new HashMap<>();

    public Builder(final String realm) {
      this.realm = requireNonNull(realm, "realm");
    }

    /**
     * Add user to the password file.
     *
     * @param userName the name of the user.
     * @param password the user's password, in plain text.
     * @param resource the resource(s) the user can access.
     * @return self
     */
    public Builder addUser(final String userName, final String password, final String resource) {
      users.put(userName, new UserEntry(password, resource));
      return this;
    }

    public TestBasicJaasConfig build() {
      try {
        final Path passwordFile = createPasswordFile();
        final Path jaasFile = createJaasFile(passwordFile);

        return new TestBasicJaasConfig(jaasFile);
      } catch (final IOException e) {
        throw new AssertionError("Failed to create JaasConfig", e);
      }
    }

    private String createPasswordFileContent() {
      return users.entrySet().stream()
          .map(e -> e.getKey() + ":" + e.getValue())
          .collect(Collectors.joining(System.lineSeparator()));
    }

    private String createJaasFileContent(final Path passwordFile) {
      return realm + " {\n  "
          + "org.eclipse.jetty.security.jaas.spi.PropertyFileLoginModule required\n"
          + "  file=\"" + passwordFile + "\"\n"
          + "  debug=\"true\";\n"
          + "};\n";
    }

    private Path createPasswordFile() throws IOException {
      final String content = createPasswordFileContent();
      final Path passwordFile = tempFile("password-file");
      Files.write(passwordFile, content.getBytes(Charsets.UTF_8));
      return passwordFile;
    }

    private Path createJaasFile(final Path passwordFile) throws IOException {
      final String content = createJaasFileContent(passwordFile);
      final Path jaasFile = tempFile("jaas-config");
      Files.write(jaasFile, content.getBytes(Charsets.UTF_8));
      return jaasFile;
    }

    private static Path tempFile(final String name) throws IOException {
      final File file = File.createTempFile(name, "." + UUID.randomUUID().toString());
      file.deleteOnExit();
      return file.toPath();
    }
  }

  @Immutable
  private static final class UserEntry {

    private final String password;
    private final String resource;

    private UserEntry(final String password, final String resource) {
      this.password = requireNonNull(password, "password");
      this.resource = requireNonNull(resource, "resource");
    }

    @Override
    public String toString() {
      return password + "," + resource;
    }
  }
}
