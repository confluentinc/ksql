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
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.tree.CreateFunction;
import io.confluent.ksql.security.ExtensionSecurityManager;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;

import org.codehaus.commons.compiler.CompileException;
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
      private final IScriptEvaluator se;

      @SuppressWarnings("checkstyle:redundantmodifier")
      public InlineJavaUdf(
          final CreateFunction createFunction,
          final KsqlConfig ksqlConfig
      ) throws Exception {
        try {
          // create a blacklist
          final File pluginDir = new File(ksqlConfig.getString(KsqlConfig.KSQL_EXT_DIR));
          final Blacklist blacklist = new Blacklist(new File(pluginDir, "resource-blacklist.txt"));

          // create a class loader
          final UdfClassLoader classLoader = UdfClassLoader.newClassLoader(
              Optional.empty(),
              Thread.currentThread().getContextClassLoader(),
              blacklist);

          // create a script executor
          se = CompilerFactoryFactory.getDefaultCompilerFactory().newScriptEvaluator();
          se.setParentClassLoader(classLoader);
          se.setReturnType(SchemaUtil.getJavaType(createFunction.getReturnType()));
          se.setParameters(createFunction.getArgumentNames(), createFunction.getArgumentTypes());
          se.cook(createFunction.getScript());

        } catch (CompileException ce) {
          final String errorMessage = String.format("Failed to compile UDF");
          ce.printStackTrace();
          throw new KsqlException(errorMessage);
        }
      }

      @SuppressWarnings("unchecked")
      @Override
      public Object evaluate(final Object... args) {
        Object value;
        try {
          ExtensionSecurityManager.INSTANCE.pushInUdf();
          value = se.evaluate(args);
        } catch (Exception e) {
          LOG.warn("Exception encountered while executing function. Setting value to null", e);
          e.printStackTrace();
          value = null;
        } finally {
          ExtensionSecurityManager.INSTANCE.popOutUdf();
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
        // instantiate the UDF
        final Constructor<? extends Kudf> constructor =
            kudfClass.getConstructor(CreateFunction.class, KsqlConfig.class);
        return constructor.newInstance(createFunction, ksqlConfig);
      } catch (Exception e) {
        e.printStackTrace();
        throw new KsqlException("Failed to instantiate UDF", e);
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
              kudfClass,
              udfFactory,
              createFunction.getDescription(),
              KsqlFunction.INTERNAL_PATH,
              false);

      final UdfMetadata metadata = new UdfMetadata(
              createFunction.getName(),
              createFunction.getOverview(),
              createFunction.getAuthor(),
              createFunction.getVersion(),
              KsqlFunction.INTERNAL_PATH,
              false,
              true);

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
