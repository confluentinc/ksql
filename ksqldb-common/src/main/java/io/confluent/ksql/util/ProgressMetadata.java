package io.confluent.ksql.util;

public class ProgressMetadata {

  private final String startToken;
  private final String endToken;

  public ProgressMetadata(String startToken, String endToken) {
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
