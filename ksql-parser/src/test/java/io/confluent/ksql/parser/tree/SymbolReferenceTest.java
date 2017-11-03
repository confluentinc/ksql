package io.confluent.ksql.parser.tree;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for class {@link SymbolReference}.
 *
 * @see SymbolReference
 */
public class SymbolReferenceTest {

  @Test
  public void testEqualsReturningTrue() {
    SymbolReference symbolReferenceOne = new SymbolReference("");
    SymbolReference symbolReferenceTwo = new SymbolReference("");

    assertTrue(symbolReferenceOne.equals(symbolReferenceTwo));
    assertTrue(symbolReferenceOne.equals(symbolReferenceOne));
  }

  @Test
  public void testEqualsReturningFalse() {
    SymbolReference symbolReference = new SymbolReference("query is null");

    assertFalse(symbolReference.equals("query is null"));
    assertFalse(symbolReference.equals(new Object()));
  }

  @Test
  public void testEqualsWithNull() {
    SymbolReference symbolReference = new SymbolReference(null);

    assertFalse(symbolReference.equals(null));
  }
}