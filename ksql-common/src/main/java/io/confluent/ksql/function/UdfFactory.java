/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.function;

import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.kafka.connect.data.Schema;

public class UdfFactory {
  private final UdfMetadata metadata;
  private final Class<? extends Kudf> udfClass;
  private final UdfIndex udfIndex;

  UdfFactory(final Class<? extends Kudf> udfClass,
             final UdfMetadata metadata) {
    this.udfClass = Objects.requireNonNull(udfClass, "udfClass can't be null");
    this.metadata = Objects.requireNonNull(metadata, "metadata can't be null");
    this.udfIndex = new UdfIndex(metadata);
  }

  void addFunction(final KsqlFunction ksqlFunction) {
    checkCompatible(ksqlFunction);
    udfIndex.addFunction(ksqlFunction);
  }

  private void checkCompatible(final KsqlFunction ksqlFunction) {
    if (udfClass != ksqlFunction.getKudfClass()) {
      throw new KsqlException("Can't add function " + ksqlFunction
          + " as a function with the same name exists in a different " + udfClass);
    }
    if (!ksqlFunction.getPathLoadedFrom().equals(metadata.getPath())) {
      throw new KsqlException("Can't add function " + ksqlFunction
          + "as a function with the same name has been loaded from a different jar "
          + metadata.getPath());
    }
  }

  public String getName() {
    return metadata.getName();
  }

  public String getAuthor() {
    return metadata.getAuthor();
  }

  public String getVersion() {
    return metadata.getVersion();
  }

  public String getDescription() {
    return metadata.getDescription();
  }

  public void eachFunction(final Consumer<KsqlFunction> consumer) {
    udfIndex.values().forEach(consumer);
  }

  public boolean isInternal() {
    return metadata.isInternal();
  }

  public String getPath() {
    return metadata.getPath();
  }

  public boolean matches(final UdfFactory that) {
    return this == that
        || (this.udfClass.equals(that.udfClass) && this.metadata.equals(that.metadata));
  }

  @Override
  public String toString() {
    return "UdfFactory{"
        + "metadata=" + metadata
        + ", udfClass=" + udfClass
        + ", udfIndex=" + udfIndex
        + '}';
  }

  public KsqlFunction getFunction(final List<Schema> paramTypes) {
    return udfIndex.getFunction(paramTypes);
  }
}
