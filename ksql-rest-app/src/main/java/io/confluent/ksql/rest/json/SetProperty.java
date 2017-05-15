package io.confluent.ksql.rest.json;

import java.util.Objects;

public class SetProperty extends KSQLStatementResponse {

  private final AdjustedProperty adjustedProperty;

  public SetProperty(String statementText, String property, Object oldValue, Object newValue) {
    super(statementText);
    this.adjustedProperty = new AdjustedProperty(property, oldValue, newValue);
  }

  public AdjustedProperty getAdjustedProperty() {
    return adjustedProperty;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SetProperty)) {
      return false;
    }
    SetProperty that = (SetProperty) o;
    return Objects.equals(getAdjustedProperty(), that.getAdjustedProperty());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getAdjustedProperty());
  }

  public static class AdjustedProperty {
    private final String property;
    private final Object oldValue;
    private final Object newValue;

    public AdjustedProperty(String property, Object oldValue, Object newValue) {
      this.property = property;
      this.oldValue = oldValue;
      this.newValue = newValue;
    }

    public String getProperty() {
      return property;
    }

    public Object getOldValue() {
      return oldValue;
    }

    public Object getNewValue() {
      return newValue;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof AdjustedProperty)) {
        return false;
      }
      AdjustedProperty that = (AdjustedProperty) o;
      return Objects.equals(getProperty(), that.getProperty()) &&
          Objects.equals(getOldValue(), that.getOldValue()) &&
          Objects.equals(getNewValue(), that.getNewValue());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getProperty(), getOldValue(), getNewValue());
    }
  }
}
