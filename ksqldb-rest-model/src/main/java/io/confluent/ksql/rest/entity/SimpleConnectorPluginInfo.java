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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.errorprone.annotations.Immutable;
import java.util.Locale;
import java.util.Objects;
import org.apache.kafka.common.config.provider.ConfigProvider;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@Immutable
public class SimpleConnectorPluginInfo {
  @JsonAlias({"class"})
  private final String className;
  private final PluginType type;
  private final String version;

  @JsonCreator
  public SimpleConnectorPluginInfo(
      @JsonProperty("className") final String className,
      @JsonProperty("type") final PluginType type,
      @JsonProperty("version") final String version
  ) {
    this.className = Objects.requireNonNull(className, "className");
    this.type = type;
    this.version = version;
  }

  public String getClassName() {
    return className;
  }

  public PluginType getType() {
    return type;
  }

  public String getVersion() {
    return version;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SimpleConnectorPluginInfo that = (SimpleConnectorPluginInfo) o;
    return Objects.equals(className, that.className)
        && type == that.type
        && Objects.equals(version, that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(className, type, version);
  }

  @Override
  public String toString() {
    return "SimpleConnectorPluginInfo{"
        + "className='" + className + '\''
        + ", type=" + type
        + ", version='" + version + '\''
        + '}';
  }

  public enum PluginType {
    SOURCE(SourceConnector.class),
    SINK(SinkConnector.class),
    CONVERTER(Converter.class),
    HEADER_CONVERTER(HeaderConverter.class),
    TRANSFORMATION(Transformation.class),
    PREDICATE(Predicate.class),
    CONFIGPROVIDER(ConfigProvider.class),
    REST_EXTENSION(ConnectRestExtension.class),
    CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY(ConnectorClientConfigOverridePolicy.class),
    UNKNOWN(Object.class);

    private final Class<?> klass;

    PluginType(final Class<?> klass) {
      this.klass = klass;
    }

    public static PluginType from(final Class<?> klass) {
      final PluginType[] var1 = values();
      final int var2 = var1.length;

      for (int var3 = 0; var3 < var2; ++var3) {
        final PluginType type = var1[var3];
        if (type.klass.isAssignableFrom(klass)) {
          return type;
        }
      }

      return UNKNOWN;
    }

    public String simpleName() {
      return this.klass.getSimpleName();
    }

    @JsonValue
    public String toString() {
      return super.toString().toLowerCase(Locale.ROOT);
    }
  }
}
