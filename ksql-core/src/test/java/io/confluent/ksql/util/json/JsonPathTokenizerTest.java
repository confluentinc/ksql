package io.confluent.ksql.util.json;


import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Test;
import java.io.IOException;
import java.util.List;

public class JsonPathTokenizerTest {

  @Test
  public void testJsonPathTokenizer() throws IOException {
    JsonPathTokenizer jsonPathTokenizer = new JsonPathTokenizer("$.log.cloud.region");
    ImmutableList<String> tokens = ImmutableList.copyOf(jsonPathTokenizer);
    List<String> tokenList = tokens.asList();
    Assert.assertTrue(tokenList.size() == 3);
    Assert.assertTrue(tokenList.get(0).equalsIgnoreCase("log"));
    Assert.assertTrue(tokenList.get(1).equalsIgnoreCase("cloud"));
    Assert.assertTrue(tokenList.get(2).equalsIgnoreCase("region"));

  }

}
