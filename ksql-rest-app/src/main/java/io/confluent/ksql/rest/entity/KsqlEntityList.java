/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.rest.entity;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Utility class to prevent type erasure from stripping annotation information from KsqlEntity
 * instances in a list
 */
public class KsqlEntityList extends ArrayList<KsqlEntity> {
  public KsqlEntityList() {
  }

  public KsqlEntityList(int initialCapacity) {
    super(initialCapacity);
  }

  public KsqlEntityList(Collection<? extends KsqlEntity> c) {
    super(c);
  }
}
