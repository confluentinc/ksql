/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Objects;

@JsonTypeName("setProperty")
public class SetProperty extends KsqlEntity {

  private final String property;
  private final Object oldValue;
  private final Object newValue;

  @JsonCreator
  public SetProperty(
      @JsonProperty("statementText") String statementText,
      @JsonProperty("property")      String property,
      @JsonProperty("oldValue")      Object oldValue,
      @JsonProperty("newValue")      Object newValue
  ) {
    super(statementText);
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
    if (!(o instanceof SetProperty)) {
      return false;
    }
    SetProperty that = (SetProperty) o;
    return Objects.equals(getProperty(), that.getProperty())
        && Objects.equals(getOldValue(), that.getOldValue())
        && Objects.equals(getNewValue(), that.getNewValue());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProperty(), getOldValue(), getNewValue());
  }
}
