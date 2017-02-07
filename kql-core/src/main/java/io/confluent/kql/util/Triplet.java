/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.util;

public class Triplet<T1, T2, T3> {

  public final T1 first;
  public final T2 second;
  public final T3 third;

  public Triplet(T1 first, T2 second, T3 third) {
    this.first = first;
    this.second = second;
    this.third = third;
  }

  public T1 getFirst() {
    return first;
  }

  public T2 getSecond() {
    return second;
  }

  public T3 getThird() {
    return third;
  }
}