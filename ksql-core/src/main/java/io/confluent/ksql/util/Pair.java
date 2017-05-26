/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.util;

public class Pair<T1, T2> {

  public final T1 left;
  public final T2 right;

  public Pair(T1 left, T2 right) {
    this.left = left;
    this.right = right;
  }

  public T1 getLeft() {
    return left;
  }

  public T2 getRight() {
    return right;
  }
}
