package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

import java.util.Map;
import java.util.Objects;

@JsonTypeName("properties")
public class PropertiesList extends KSQLEntity {
  private final Map<String, Object> properties;

  public PropertiesList(String statementText, Map<String, Object> properties) {
    super(statementText);
    this.properties = properties;
  }

  @JsonUnwrapped
  public Map<String, Object> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PropertiesList)) {
      return false;
    }
    PropertiesList that = (PropertiesList) o;
    return Objects.equals(getProperties(), that.getProperties());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProperties());
  }
}
