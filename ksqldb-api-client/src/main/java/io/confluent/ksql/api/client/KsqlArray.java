/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.api.client;

import io.vertx.core.json.JsonArray;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class KsqlArray {

  private final JsonArray delegate;

  public KsqlArray() {
    delegate = new JsonArray();
  }

  public KsqlArray(final List<?> list) {
    delegate = new JsonArray(list);
  }

  KsqlArray(final JsonArray jsonArray) {
    delegate = Objects.requireNonNull(jsonArray);
  }

  public boolean contains(final Object value) {
    return delegate.contains(value);
  }

  public int size() {
    return delegate.size();
  }

  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  public List<?> getList() {
    return delegate.getList();
  }

  public Iterator<Object> iterator() {
    return delegate.iterator();
  }

  public java.util.stream.Stream<Object> stream() {
    return delegate.stream();
  }

  public Object getValue(final int pos) {
    return delegate.getValue(pos);
  }

  public String getString(final int pos) {
    return delegate.getString(pos);
  }

  public Integer getInteger(final int pos) {
    return delegate.getInteger(pos);
  }

  public Long getLong(final int pos) {
    return delegate.getLong(pos);
  }

  public Double getDouble(final int pos) {
    return delegate.getDouble(pos);
  }

  public Boolean getBoolean(final int pos) {
    return delegate.getBoolean(pos);
  }

  public KsqlArray getKsqlArray(final int pos) {
    return new KsqlArray(delegate.getJsonArray(pos));
  }

  public KsqlObject getKsqlObject(final int pos) {
    return new KsqlObject(delegate.getJsonObject(pos));
  }

  public Object remove(final int pos) {
    return delegate.remove(pos);
  }

  public Object remove(final Object value) {
    return delegate.remove(value);
  }

  public KsqlArray add(final String value) {
    delegate.add(value);
    return this;
  }

  public KsqlArray add(final Integer value) {
    delegate.add(value);
    return this;
  }

  public KsqlArray add(final Long value) {
    delegate.add(value);
    return this;
  }

  public KsqlArray add(final Double value) {
    delegate.add(value);
    return this;
  }

  public KsqlArray add(final Boolean value) {
    delegate.add(value);
    return this;
  }

  public KsqlArray add(final KsqlArray value) {
    delegate.add(value);
    return this;
  }

  public KsqlArray add(final KsqlObject value) {
    delegate.add(value);
    return this;
  }

  public KsqlArray add(final Object value) {
    delegate.add(value);
    return this;
  }

  public KsqlArray addNull() {
    delegate.addNull();
    return this;
  }

  public KsqlArray addAll(final KsqlArray array) {
    delegate.addAll(toJsonArray(array));
    return this;
  }

  public KsqlArray copy() {
    return new KsqlArray(delegate.copy());
  }

  public String toJsonString() {
    return delegate.toString();
  }

  @Override
  public String toString() {
    return toJsonString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KsqlArray that = (KsqlArray) o;
    return delegate.equals(that.delegate);
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  private static JsonArray toJsonArray(final KsqlArray ksqlArray) {
    return new JsonArray(ksqlArray.getList());
  }
}
