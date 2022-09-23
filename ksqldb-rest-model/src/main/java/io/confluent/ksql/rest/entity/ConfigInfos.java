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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConfigInfos {
  public static final String CONNECTOR_CLASS_CONFIG = "connector.class";

  @JsonProperty("name")
  private final String name;
  @JsonProperty("error_count")
  private final int errorCount;
  @JsonProperty("groups")
  private final ImmutableList<String> groups;
  @JsonProperty("configs")
  private final ImmutableList<ConfigInfo> configs;

  @JsonCreator
  public ConfigInfos(
      @JsonProperty("name") final String name,
      @JsonProperty("error_count") final int errorCount,
      @JsonProperty("groups") final List<String> groups,
      @JsonProperty("configs") final List<ConfigInfo> configs
  ) {
    this.name = name;
    this.errorCount = errorCount;
    this.groups = ImmutableList.copyOf(groups);
    this.configs = ImmutableList.copyOf(configs);
  }

  @JsonProperty
  public String name() {
    return this.name;
  }

  @JsonProperty
  public int errorCount() {
    return this.errorCount;
  }

  @JsonProperty
  public List<String> groups() {
    return this.groups;
  }

  @JsonProperty("configs")
  public List<ConfigInfo> values() {
    return this.configs;
  }

  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    } else if (o != null && this.getClass() == o.getClass()) {
      final ConfigInfos that = (ConfigInfos) o;
      return Objects.equals(this.name, that.name)
          && Objects.equals(this.errorCount, that.errorCount)
          && Objects.equals(this.groups, that.groups)
          && Objects.equals(this.configs, that.configs);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hash(new Object[]{this.name, this.errorCount, this.groups, this.configs});
  }

  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("[").append(this.name).append(",").append(this.errorCount).append(",")
        .append(this.groups).append(",").append(this.configs).append("]");
    return sb.toString();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ConfigInfo {
    private final ConfigKeyInfo configKey;
    private final ConfigValueInfo configValue;

    @JsonCreator
    public ConfigInfo(
        @JsonProperty("definition") final ConfigKeyInfo configKey,
        @JsonProperty("value") final ConfigValueInfo configValue
    ) {
      this.configKey = configKey;
      this.configValue = configValue;
    }

    @JsonProperty("definition")
    public ConfigKeyInfo configKey() {
      return this.configKey;
    }

    @JsonProperty("value")
    public ConfigValueInfo configValue() {
      return this.configValue;
    }

    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      } else if (o != null && this.getClass() == o.getClass()) {
        final ConfigInfo that = (ConfigInfo) o;
        return Objects.equals(this.configKey, that.configKey)
            && Objects.equals(this.configValue, that.configValue);
      } else {
        return false;
      }
    }

    public int hashCode() {
      return Objects.hash(new Object[]{this.configKey, this.configValue});
    }

    public String toString() {
      return "[" + this.configKey + "," + this.configValue + "]";
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ConfigKeyInfo {
    private final String name;
    private final String type;
    private final boolean required;
    private final String defaultValue;
    private final String importance;
    private final String documentation;
    private final String group;
    private final int orderInGroup;
    private final String width;
    private final String displayName;
    private final ImmutableList<String> dependents;

    // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
    @JsonCreator
    public ConfigKeyInfo(
        @JsonProperty("name") final String name,
        @JsonProperty("type") final String type,
        @JsonProperty("required") final boolean required,
        @JsonProperty("default_value") final String defaultValue,
        @JsonProperty("importance") final String importance,
        @JsonProperty("documentation") final String documentation,
        @JsonProperty("group") final String group,
        @JsonProperty("order_in_group") final int orderInGroup,
        @JsonProperty("width") final String width,
        @JsonProperty("display_name") final String displayName,
        @JsonProperty("dependents") final List<String> dependents
    ) {
      // CHECKSTYLE_RULES.ON: ParameterNumberCheck
      this.name = name;
      this.type = type;
      this.required = required;
      this.defaultValue = defaultValue;
      this.importance = importance;
      this.documentation = documentation;
      this.group = group;
      this.orderInGroup = orderInGroup;
      this.width = width;
      this.displayName = displayName;
      this.dependents = ImmutableList.copyOf(dependents);
    }

    @JsonProperty
    public String name() {
      return this.name;
    }

    @JsonProperty
    public String type() {
      return this.type;
    }

    @JsonProperty
    public boolean required() {
      return this.required;
    }

    @JsonProperty("default_value")
    public String defaultValue() {
      return this.defaultValue;
    }

    @JsonProperty
    public String documentation() {
      return this.documentation;
    }

    @JsonProperty
    public String group() {
      return this.group;
    }

    @JsonProperty("order")
    public int orderInGroup() {
      return this.orderInGroup;
    }

    @JsonProperty
    public String width() {
      return this.width;
    }

    @JsonProperty
    public String importance() {
      return this.importance;
    }

    @JsonProperty("display_name")
    public String displayName() {
      return this.displayName;
    }

    @JsonProperty
    public List<String> dependents() {
      return this.dependents;
    }

    // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
    public boolean equals(final Object o) {
      // CHECKSTYLE_RULES.ON: CyclomaticComplexity
      if (this == o) {
        return true;
      } else if (o != null && this.getClass() == o.getClass()) {
        final ConfigKeyInfo that = (ConfigKeyInfo) o;
        return Objects.equals(this.name, that.name)
            && Objects.equals(this.type, that.type)
            && Objects.equals(this.required, that.required)
            && Objects.equals(this.defaultValue, that.defaultValue)
            && Objects.equals(this.importance, that.importance)
            && Objects.equals(this.documentation, that.documentation)
            && Objects.equals(this.group, that.group)
            && Objects.equals(this.orderInGroup, that.orderInGroup)
            && Objects.equals(this.width, that.width)
            && Objects.equals(this.displayName, that.displayName)
            && Objects.equals(this.dependents, that.dependents);
      } else {
        return false;
      }
    }

    public int hashCode() {
      return Objects.hash(new Object[]{this.name, this.type, this.required, this.defaultValue,
          this.importance, this.documentation, this.group, this.orderInGroup, this.width,
          this.displayName, this.dependents});
    }

    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append("[").append(this.name).append(",")
          .append(this.type).append(",")
          .append(this.required).append(",")
          .append(this.defaultValue).append(",")
          .append(this.importance).append(",")
          .append(this.documentation).append(",")
          .append(this.group).append(",")
          .append(this.orderInGroup).append(",")
          .append(this.width).append(",")
          .append(this.displayName).append(",")
          .append(this.dependents).append("]");
      return sb.toString();
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ConfigValueInfo {
    private final String name;
    private final String value;
    private final ImmutableList<String> recommendedValues;
    private final ImmutableList<String> errors;
    private final boolean visible;

    @JsonCreator
    public ConfigValueInfo(
        @JsonProperty("name") final String name,
        @JsonProperty("value") final String value,
        @JsonProperty("recommended_values") final List<String> recommendedValues,
        @JsonProperty("errors") final List<String> errors,
        @JsonProperty("visible") final boolean visible
    ) {
      this.name = name;
      this.value = value;
      this.recommendedValues = ImmutableList.copyOf(recommendedValues);
      this.errors = ImmutableList.copyOf(errors);
      this.visible = visible;
    }

    @JsonProperty
    public String name() {
      return this.name;
    }

    @JsonProperty
    public String value() {
      return this.value;
    }

    @JsonProperty
    public List<String> recommendedValues() {
      return this.recommendedValues;
    }

    @JsonProperty
    public List<String> errors() {
      return this.errors;
    }

    @JsonProperty
    public boolean visible() {
      return this.visible;
    }

    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      } else if (o != null && this.getClass() == o.getClass()) {
        final ConfigValueInfo that = (ConfigValueInfo) o;
        return Objects.equals(this.name, that.name)
            && Objects.equals(this.value, that.value)
            && Objects.equals(this.recommendedValues, that.recommendedValues)
            && Objects.equals(this.errors, that.errors)
            && Objects.equals(this.visible, that.visible);
      } else {
        return false;
      }
    }

    public int hashCode() {
      return Objects.hash(
          new Object[]{this.name, this.value, this.recommendedValues, this.errors, this.visible});
    }

    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append("[").append(this.name).append(",")
          .append(this.value).append(",")
          .append(this.recommendedValues).append(",")
          .append(this.errors).append(",")
          .append(this.visible).append("]");
      return sb.toString();
    }
  }
}
