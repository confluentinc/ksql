/*
 * Copyright 2022 Confluent Inc.
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
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

public class PluginInfo {
  private final String className;
  private final PluginType type;
  private final String version;

  @JsonCreator
  public PluginInfo(
      @JsonProperty("class") final String className,
      @JsonProperty("type") final PluginType type,
      @JsonProperty("version") final String version
  ) {
    this.className = className;
    this.type = type;
    this.version = version;
  }

  @JsonProperty("class")
  public String className() {
    return this.className;
  }

  @JsonProperty("type")
  public String type() {
    return this.type.toString();
  }

  @JsonProperty("version")
  @JsonInclude(
      value = Include.CUSTOM,
      valueFilter = NoVersionFilter.class
  )
  public String version() {
    return this.version;
  }

  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    } else if (o != null && this.getClass() == o.getClass()) {
      final PluginInfo that = (PluginInfo) o;
      return Objects.equals(this.className, that.className)
          && Objects.equals(this.type, that.type)
          && Objects.equals(this.version, that.version);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hash(new Object[]{this.className, this.type, this.version});
  }

  public String toString() {
    return "PluginInfo{className='" + this.className + '\'' + ", type=" + this.type.toString()
        + ", version='" + this.version + '\'' + '}';
  }

  public static final class NoVersionFilter {
    public NoVersionFilter() {
    }

    public boolean equals(final Object obj) {
      return "undefined".equals(obj);
    }

    public int hashCode() {
      return super.hashCode();
    }
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

    public String toString() {
      return super.toString().toLowerCase(Locale.ROOT);
    }
  }
}
