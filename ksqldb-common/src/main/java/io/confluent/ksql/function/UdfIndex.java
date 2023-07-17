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

package io.confluent.ksql.function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.GenericType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An index of all method signatures associated with a UDF. This index
 * supports lookups based on method signatures, defined by a list of
 * nullable {@link ParamType}.
 *
 * <p>Resolving a method signature takes the following precedence rules:
 * <ul>
 *   <li>If only one exact method exists, return it</li>
 *   <li>If a method exists such that all non-null {@code Schema} in the
 *       parameters match, and all other parameters are optional, return
 *       that method</li>
 *   <li>If two methods exist that match given the above rules, and one
 *       does not have variable arguments (e.g. {@code String...}, return
 *       that one.</li>
 *   <li>If two methods exist that match given the above rules, and both
 *       have variable arguments, return the method with the more non-variable
 *       arguments.</li>
 *   <li>If two methods exist that match only null values, return the one
 *       that was added first.</li>
 * </ul>
 */
public class UdfIndex<T extends FunctionSignature> {
  // this class is implemented as a custom Trie data structure that resolves
  // the rules described above. the Trie is built so that each node in the
  // trie references a single possible parameter in the signature. take for
  // example the following method signatures:
  //
  // A: void foo(String a)
  // B: void foo(String a, Integer b)
  // C: void foo(String a, Integer... ints)
  // D: void foo(Integer a)
  // E: void foo(Integer... ints)
  //
  // The final constructed trie would look like:
  //
  //                 Ø -> E
  //               /   \
  //    A <-- String   Integer -> D
  //         /            \
  // B <-- Integer      Integer (VARARG) -> E
  //        /
  // C <-- Integer (VARARG)
  //
  // To resolve a query against the trie, this class simply traverses down each
  // matching path of the trie until it exhausts the input List<Schema>. The
  // final node that it reaches will contain the method that is returned. If
  // multiple methods are returned, the rules described above are used to select
  // the best candidate (e.g. foo(null, int) matches B, C and E).

  private static final Logger LOG = LoggerFactory.getLogger(UdfIndex.class);

  private final String udfName;
  private final Node root = new Node();
  private final Map<List<ParamType>, T> allFunctions;
  private final boolean supportsImplicitCasts;

  UdfIndex(final String udfName, final boolean supportsImplicitCasts) {
    this.udfName = Objects.requireNonNull(udfName, "udfName");
    this.allFunctions = new HashMap<>();
    this.supportsImplicitCasts = supportsImplicitCasts;
  }

  void addFunction(final T function) {
    final List<ParamType> parameters = function.parameters();
    if (allFunctions.put(parameters, function) != null) {
      throw new KsqlFunctionException(
          "Can't add function " + function.name()
              + " with parameters " + function.parameters()
              + " as a function with the same name and parameter types already exists "
              + allFunctions.get(parameters)
      );
    }

    final int order = allFunctions.size();

    Node curr = root;
    Node parent = curr;
    for (final ParamType parameter : parameters) {
      final Parameter param = new Parameter(parameter, false);
      parent = curr;
      curr = curr.children.computeIfAbsent(param, ignored -> new Node());
    }

    if (function.isVariadic()) {
      // first add the function to the parent to address the
      // case of empty varargs
      parent.update(function, order);

      // then add a new child node with the parameter value type
      // and add this function to that node
      final ParamType varargSchema = Iterables.getLast(parameters);
      final Parameter vararg = new Parameter(varargSchema, true);
      final Node leaf = parent.children.computeIfAbsent(vararg, ignored -> new Node());
      leaf.update(function, order);

      // add a self referential loop for varargs so that we can
      // add as many of the same param at the end and still retrieve
      // this node
      leaf.children.putIfAbsent(vararg, leaf);
    }

    curr.update(function, order);
  }

  T getFunction(final List<SqlArgument> arguments) {
    final List<Node> candidates = new ArrayList<>();

    // first try to get the candidates without any implicit casting
    getCandidates(arguments, 0, root, candidates, new HashMap<>(), false);
    final Optional<T> fun = candidates
        .stream()
        .max(Node::compare)
        .map(node -> node.value);

    if (fun.isPresent()) {
      return fun.get();
    } else if (!supportsImplicitCasts) {
      throw createNoMatchingFunctionException(arguments);
    }

    // if none were found (candidates is empty) try again with
    // implicit casting
    getCandidates(arguments, 0, root, candidates, new HashMap<>(), true);
    return candidates
        .stream()
        .max(Node::compare)
        .map(node -> node.value)
        .orElseThrow(() -> createNoMatchingFunctionException(arguments));
  }

  private void getCandidates(
      final List<SqlArgument> arguments,
      final int argIndex,
      final Node current,
      final List<Node> candidates,
      final Map<GenericType, SqlType> reservedGenerics,
      final boolean allowCasts
  ) {
    if (argIndex == arguments.size()) {
      if (current.value != null) {
        candidates.add(current);
      }
      return;
    }

    final SqlArgument arg = arguments.get(argIndex);
    for (final Entry<Parameter, Node> candidate : current.children.entrySet()) {
      final Map<GenericType, SqlType> reservedCopy = new HashMap<>(reservedGenerics);
      if (candidate.getKey().accepts(arg, reservedCopy, allowCasts)) {
        final Node node = candidate.getValue();
        getCandidates(arguments, argIndex + 1, node, candidates, reservedCopy, allowCasts);
      }
    }
  }

  private KsqlException createNoMatchingFunctionException(final List<SqlArgument> paramTypes) {
    LOG.debug("Current UdfIndex:\n{}", describe());

    final String requiredTypes = paramTypes.stream()
        .map(argument -> {
          if (argument == null) {
            return "null";
          } else {
            return argument.toString(FormatOptions.noEscape());
          }
        })
        .collect(Collectors.joining(", ", "(", ")"));

    final String acceptedTypes = allFunctions.values().stream()
        .map(UdfIndex::formatAvailableSignatures)
        .collect(Collectors.joining(System.lineSeparator()));

    return new KsqlException("Function '" + udfName
        + "' does not accept parameters " + requiredTypes + "."
        + System.lineSeparator()
        + "Valid alternatives are:"
        + System.lineSeparator()
        + acceptedTypes
        + System.lineSeparator()
        + "For detailed information on a function run: DESCRIBE FUNCTION <Function-Name>;"
    );
  }

  public Collection<T> values() {
    return allFunctions.values();
  }

  private String describe() {
    final StringBuilder sb = new StringBuilder();
    sb.append("-ROOT\n");
    root.describe(sb, 1);
    return sb.toString();
  }

  private static <T extends FunctionSignature> String formatAvailableSignatures(final T function) {
    final boolean variadicFunction = function.isVariadic();
    final List<ParameterInfo> parameters = function.parameterInfo();

    final StringBuilder result = new StringBuilder();
    result.append(function.name().toString(FormatOptions.noEscape())).append("(");

    for (int i = 0; i < parameters.size(); i++) {
      final ParameterInfo param = parameters.get(i);
      final boolean variadicParam = variadicFunction && i == (parameters.size() - 1);
      final String type = variadicParam
          ? ((ArrayType) param.type()).element().toString() + "..."
          : param.type().toString();

      if (i != 0) {
        result.append(", ");
      }

      result.append(type);

      if (!param.name().isEmpty()) {
        result.append(" ").append(param.name());
      }
    }

    return result.append(")").toString();
  }

  private final class Node {

    @VisibleForTesting
    private final Comparator<T> compareFunctions =
        Comparator.nullsFirst(
            Comparator
                .<T, Integer>comparing(fun -> fun.isVariadic() ? 0 : 1)
                .thenComparing(fun -> fun.parameters().size())
        );

    private final Map<Parameter, Node> children;
    private T value;
    private int order = 0;

    private Node() {
      this.children = new HashMap<>();
      this.value = null;
    }

    private void update(final T function, final int order) {
      if (compareFunctions.compare(function, value) > 0) {
        value = function;
        this.order = order;
      }
    }

    private void describe(final StringBuilder builder, final int indent) {
      for (final Entry<Parameter, Node> child : children.entrySet()) {
        if (child.getValue() != this) {
          builder.append(StringUtils.repeat(' ', indent * 2))
              .append('-')
              .append(child.getKey())
              .append(": ")
              .append(child.getValue())
              .append('\n');
          child.getValue().describe(builder, indent + 1);
        }
      }
    }

    @Override
    public String toString() {
      return value != null ? value.name().text() : "EMPTY";
    }

    int compare(final Node other) {
      final int compare = compareFunctions.compare(value, other.value);
      return compare == 0 ? -(order - other.order) : compare;
    }

  }

  /**
   * A class that represents a parameter, with a schema and whether
   * or not it is part of a variable argument declaration.
   */
  private static final class Parameter {

    private final ParamType type;
    private final boolean isVararg;

    private Parameter(final ParamType type, final boolean isVararg) {
      this.isVararg = isVararg;
      this.type = isVararg ? ((ArrayType) type).element() : type;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Parameter parameter = (Parameter) o;
      return isVararg == parameter.isVararg
          && Objects.equals(type, parameter.type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, isVararg);
    }

    /**
     * @param argument         the argument to test against
     * @param reservedGenerics a mapping of generics to already reserved types - this map
     *                         will be updated if the parameter is generic to point to the
     *                         current argument for future checks to accept
     * @param allowCasts       whether or not to accept an implicit cast
     * @return whether or not this argument can be used as a value for
     *         this parameter
     */
    // CHECKSTYLE_RULES.OFF: BooleanExpressionComplexity
    boolean accepts(final SqlArgument argument, final Map<GenericType, SqlType> reservedGenerics,
        final boolean allowCasts) {
      if (argument == null
          || (!argument.getSqlLambda().isPresent() && !argument.getSqlType().isPresent())) {
        return true;
      }

      if (GenericsUtil.hasGenerics(type)) {
        return GenericsUtil.reserveGenerics(type, argument, reservedGenerics).getLeft();
      }

      return ParamTypes.areCompatible(argument, type, allowCasts);
    }

    @Override
    public String toString() {
      return type + (isVararg ? "(VARARG)" : "");
    }
  }
}
