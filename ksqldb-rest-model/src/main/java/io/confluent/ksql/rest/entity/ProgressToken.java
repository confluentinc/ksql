package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ProgressToken {

  private final String startToken;
  private final String endToken;

  @JsonCreator
  public ProgressToken(
      final @JsonProperty(value = "startToken", required = true) String startToken,
      final @JsonProperty(value = "endToken", required = true) String endToken) {
    this.startToken = startToken;
    this.endToken = endToken;
  }

  public String getStartToken() {
    return startToken;
  }

  public String getEndToken() {
    return endToken;
  }
}
