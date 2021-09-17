package io.confluent.ksql.util;

public class RowMetadata {

  private final String startToken;
  private final String endToken;

  public RowMetadata(String startToken, String endToken) {
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
