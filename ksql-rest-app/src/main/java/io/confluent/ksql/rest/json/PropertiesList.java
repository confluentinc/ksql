package io.confluent.ksql.rest.json;

import java.util.Map;
import java.util.Objects;

public class PropertiesList extends KSQLStatementResponse {
  private final Map<String, Object> properties;

  public PropertiesList(String statementText, Map<String, Object> properties) {
    super(statementText);
    this.properties = properties;
  }

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
