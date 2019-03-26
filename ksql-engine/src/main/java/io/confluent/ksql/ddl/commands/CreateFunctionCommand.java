/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.ddl.commands;

import io.confluent.ksql.function.Blacklist;
import io.confluent.ksql.function.KsqlFunction;
import io.confluent.ksql.function.MutableFunctionRegistry;
import io.confluent.ksql.function.UdfClassLoader;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.UdfInvoker;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.PluggableUdf;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.tree.CreateFunction;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;

import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.language;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class CreateFunctionCommand implements DdlCommand {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger LOG = LoggerFactory.getLogger(CreateFunctionCommand.class);

  private final CreateFunction createFunction;

  CreateFunctionCommand(
      final CreateFunction createFunction
  ) {

    this.createFunction = createFunction;
  }

  @Override
  public String toString() {
    return createFunction.getName();
  }

  /**
   * A method for retrieving a custom Kudf class. This class gets instantiated
   * everytime a query is created with the custom function.
   */
  static Class<? extends Kudf> getKudfClass(final String language) {

    class InlineJavaUdf implements Kudf {
      private final String[] argumentNames;
      private final Class[] argumentTypes;
      private final String language;
      private final String name;
      private final Class returnType;
      private final String script;
      private final UdfInvoker udfInvoker;

      @SuppressWarnings("checkstyle:redundantmodifier")
      public InlineJavaUdf(final CreateFunction cf, final UdfInvoker udfInvoker) throws Exception {
        this.argumentNames = cf.getArgumentNames();
        this.argumentTypes = cf.getArgumentTypes();
        this.language = cf.getLanguage();
        this.name = cf.getName();
        this.returnType = SchemaUtil.getJavaType(cf.getReturnType());
        this.script = cf.getScript();
        this.udfInvoker = udfInvoker;
      }

      @SuppressWarnings("unchecked")
      @Override
      public Object evaluate(final Object... args) {
        Object value;
        try {
          value = udfInvoker.eval(this, args);
        } catch (Exception e) {
          LOG.warn("Exception encountered while executing function. Setting value to null", e);
          value = null;
        }
        return value;
      }
    }

    // If / when support for multilingual UDFs is added, we should be able to
    // implement another class similar to InlineJavaUdf (e.g. MultilingualUdf), and
    // add a condition here to return the appropriate class depending on which
    // language
    return InlineJavaUdf.class;
  }

  Function<KsqlConfig, Kudf> getUdfFactory(final Class<? extends Kudf> kudfClass) {
    return ksqlConfig -> {
      try {
        // create a blacklist
        final File pluginDir = new File(ksqlConfig.getString(KsqlConfig.KSQL_EXT_DIR));
        final Blacklist blacklist = new Blacklist(new File(pluginDir, "resource-blacklist.txt"));

        // create a class loader
        final UdfClassLoader classLoader = UdfClassLoader.newClassLoader(
            Optional.empty(),
            Thread.currentThread().getContextClassLoader(),
            blacklist);

        final IScriptEvaluator scriptEvaluator
            = CompilerFactoryFactory.getDefaultCompilerFactory().newScriptEvaluator();

        scriptEvaluator.setParentClassLoader(classLoader);

        // create the UdfInvoker
        final UdfInvoker udfInvoker = (UdfInvoker) scriptEvaluator
              .createFastEvaluator(
                  createFunction.getScript(),
                  UdfInvoker.class,
                  new String[]{"thiz", "args"});

        // instantiate the UDF
        final Constructor<? extends Kudf> constructor =
            kudfClass.getConstructor(CreateFunction.class, UdfInvoker.class);
        final Kudf kudf = constructor.newInstance(createFunction, udfInvoker);

        // create a pluggable UDF, which uses a security manager to block System.exit calls
        return new PluggableUdf(udfInvoker, kudf);

      } catch (Exception e) {
        final String errorMessage = String.format("Failed to instantiate UDF. %s", e.getClass());
        throw new KsqlException(errorMessage, e);
      }
    };
  }

  @Override
  public DdlCommandResult run(final MutableMetaStore metaStore) {
    try {
      final MutableFunctionRegistry functionRegistry = metaStore.getFunctionRegistry();

      final Class<? extends Kudf> kudfClass = getKudfClass(createFunction.getLanguage());
      final Function<KsqlConfig, Kudf> udfFactory = getUdfFactory(kudfClass);
      // sanity check
      udfFactory.apply(new KsqlConfig(Collections.emptyMap()));

      final KsqlFunction ksqlFunction = KsqlFunction.create(
              createFunction.getReturnType(),
              createFunction.getArguments(),
              createFunction.getName(),
              PluggableUdf.class,
              udfFactory,
              createFunction.getDescription(),
              KsqlFunction.INTERNAL_PATH);

      final UdfMetadata metadata = new UdfMetadata(
              createFunction.getName(),
              createFunction.getOverview(),
              createFunction.getAuthor(),
              createFunction.getVersion(),
              KsqlFunction.INTERNAL_PATH,
              false,
              false);

      functionRegistry.ensureFunctionFactory(new UdfFactory(ksqlFunction.getKudfClass(), metadata));

      if (createFunction.shouldReplace()) {
        functionRegistry.addOrReplaceFunction(ksqlFunction);
      } else {
        functionRegistry.addFunction(ksqlFunction);
      }

      return new DdlCommandResult(true, "Function created");

    } catch (Exception e) {
      final String errorMessage =
            String.format("Cannot create function '%s': %s",
                createFunction.getName(), e.getMessage());
      throw new KsqlException(errorMessage, e);
    }
  }
}