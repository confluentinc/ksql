/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.util;

import javax.annotation.Nullable;

public class KsqlPreconditions {

  public static <T> T checkNotNull(T reference, @Nullable Object errorMessage) {
    if (reference == null) {
      throw new KsqlException(String.valueOf(errorMessage));
    } else {
      return reference;
    }
  }

  public static void checkArgument(boolean expression, @Nullable Object errorMessage) {
    if (!expression) {
      throw new KsqlException(String.valueOf(errorMessage));
    }
  }

}