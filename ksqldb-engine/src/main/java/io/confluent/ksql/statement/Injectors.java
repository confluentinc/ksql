/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.statement;

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.format.DefaultFormatInjector;
import io.confluent.ksql.schema.ksql.inference.DefaultSchemaInjector;
import io.confluent.ksql.schema.ksql.inference.SchemaRegisterInjector;
import io.confluent.ksql.schema.ksql.inference.SchemaRegistryTopicSchemaSupplier;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.topic.TopicCreateInjector;
import io.confluent.ksql.topic.TopicDeleteInjector;
import java.util.Objects;
import java.util.function.BiFunction;

public enum Injectors implements BiFunction<KsqlExecutionContext, ServiceContext, Injector> {

  NO_TOPIC_DELETE((ec, sc) -> InjectorChain.of(
      new DefaultFormatInjector(),
      new SourcePropertyInjector(),
      new DefaultSchemaInjector(
          new SchemaRegistryTopicSchemaSupplier(sc.getSchemaRegistryClient()), ec, sc),
      new TopicCreateInjector(ec, sc),
      new SchemaRegisterInjector(ec, sc)
  )),

  DEFAULT((ec, sc) -> InjectorChain.of(
      NO_TOPIC_DELETE.apply(ec, sc),
      new TopicDeleteInjector(ec, sc)
  ))
  ;

  private final BiFunction<KsqlExecutionContext, ServiceContext, Injector> injectorFactory;

  Injectors(final BiFunction<KsqlExecutionContext, ServiceContext, Injector> injectorFactory) {
    this.injectorFactory = Objects.requireNonNull(injectorFactory, "injectorFactory");
  }

  @Override
  public Injector apply(final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext) {
    return injectorFactory.apply(executionContext, serviceContext);
  }
}