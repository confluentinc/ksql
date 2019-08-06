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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An index of all method signatures associated with a UDF. This index
 * supports lookups based on method signatures, defined by a list of
 * nullable {@link Schema}.
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
public class UdfIndex<T extends IndexedFunction> {
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
  private static final SqlSchemaFormatter FORMATTER =
      new UdfSchemaFormatter(word -> false);

  private final String udfName;
  private final Node root = new Node();
  private final Map<List<Schema>, T> allFunctions;

  UdfIndex(final String udfName) {
    this.udfName = Objects.requireNonNull(udfName, "udfName");
    allFunctions = new HashMap<>();
  }

  void addFunction(final T function) {
    final List<Schema> parameters = function.getArguments();
    if (allFunctions.put(function.getArguments(), function) != null) {
      throw new KsqlException(
          "Can't add function "
              + function
              + " as a function with the same name and argument type already exists "
              + allFunctions.get(parameters)
      );
    }

    final int order = allFunctions.size();

    Node curr = root;
    Node parent = curr;
    for (final Schema parameter : parameters) {
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
      final Schema varargSchema = Iterables.getLast(function.getArguments());
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

  T getFunction(final List<Schema> arguments) {
    final List<Node> candidates = new ArrayList<>();
    getCandidates(arguments, 0, root, candidates, new HashMap<>());

    return candidates
        .stream()
        .max(Node::compare)
        .map(node -> node.value)
        .orElseThrow(() -> createNoMatchingFunctionException(arguments));
  }

  private void getCandidates(
      final List<Schema> arguments,
      final int argIndex,
      final Node current,
      final List<Node> candidates,
      final Map<Schema, Schema> reservedGenerics
  ) {
    if (argIndex == arguments.size()) {
      if (current.value != null) {
        candidates.add(current);
      }
      return;
    }

    final Schema arg = arguments.get(argIndex);
    for (final Entry<Parameter, Node> candidate : current.children.entrySet()) {
      final Map<Schema, Schema> reservedCopy = new HashMap<>(reservedGenerics);
      if (candidate.getKey().accepts(arg, reservedCopy)) {
        final Node node = candidate.getValue();
        getCandidates(arguments, argIndex + 1, node, candidates, reservedCopy);
      }
    }
  }

  private KsqlException createNoMatchingFunctionException(final List<Schema> paramTypes) {
    LOG.debug("Current UdfIndex:\n{}", describe());

    final String sqlParamTypes = paramTypes.stream()
        .map(schema -> schema == null
            ? null
            : FORMATTER.format(schema))
        .collect(Collectors.joining(", ", "[", "]"));

    return new KsqlException("Function '" + udfName
        + "' does not accept parameters of types:" + sqlParamTypes);
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

  private final class Node {

    @VisibleForTesting
    private final Comparator<T> compareFunctions =
        Comparator.nullsFirst(
            Comparator
                .<T, Integer>comparing(fun -> fun.isVariadic() ? 0 : 1)
                .thenComparing(fun -> fun.getArguments().size())
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
      return value != null ? value.getFunctionName() : "EMPTY";
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
  static final class Parameter {

    private static final Map<Type, BiPredicate<Schema, Schema>> CUSTOM_SCHEMA_EQ =
        ImmutableMap.<Type, BiPredicate<Schema, Schema>>builder()
            .put(Type.MAP, Parameter::mapEquals)
            .put(Type.ARRAY, Parameter::arrayEquals)
            .put(Type.STRUCT, Parameter::structEquals)
            .build();

    private final Schema schema;
    private final boolean isVararg;

    private Parameter(final Schema schema, final boolean isVararg) {
      this.isVararg = isVararg;
      this.schema = Objects.requireNonNull(isVararg ? schema.valueSchema() : schema, "schema");
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
          && Objects.equals(schema, parameter.schema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(schema, isVararg);
    }

    /**
     * @param argument         the argument to test against
     * @param reservedGenerics a mapping of generics to already reserved types - this map
     *                         will be updated if the parameter is generic to point to the
     *                         current argument for future checks to accept
     *
     * @return whether or not this argument can be used as a value for
     *         this parameter
     */
    // CHECKSTYLE_RULES.OFF: BooleanExpressionComplexity
    boolean accepts(final Schema argument, final Map<Schema, Schema> reservedGenerics) {
      if (argument == null) {
        return schema.isOptional();
      }

      if (GenericsUtil.hasGenerics(schema)) {
        return reserveGenerics(schema, argument, reservedGenerics);
      }

      final Schema.Type type = schema.type();

      // we require a custom equals method that ignores certain values (e.g.
      // whether or not the schema is optional, and the documentation)
      return Objects.equals(type, argument.type())
          && CUSTOM_SCHEMA_EQ.getOrDefault(type, (a, b) -> true).test(schema, argument)
          && Objects.equals(schema.version(), argument.version())
          && Objects.equals(schema.parameters(), argument.parameters())
          && Objects.deepEquals(schema.defaultValue(), argument.defaultValue());
    }
    // CHECKSTYLE_RULES.ON: BooleanExpressionComplexity

    private static boolean reserveGenerics(
        final Schema schema,
        final Schema argument,
        final Map<Schema, Schema> reservedGenerics
    ) {
      if (!GenericsUtil.instanceOf(schema, argument)) {
        return false;
      }

      final Map<Schema, Schema> genericMapping = GenericsUtil.resolveGenerics(schema, argument);
      for (final Entry<Schema, Schema> entry : genericMapping.entrySet()) {
        final Schema old = reservedGenerics.putIfAbsent(entry.getKey(), entry.getValue());
        if (old != null && !old.equals(entry.getValue())) {
          return false;
        }
      }

      return true;
    }

    private static boolean mapEquals(final Schema mapA, final Schema mapB) {
      return Objects.equals(mapA.keySchema(), mapB.keySchema())
          && Objects.equals(mapA.valueSchema(), mapB.valueSchema());
    }

    private static boolean arrayEquals(final Schema arrayA, final Schema arrayB) {
      return Objects.equals(arrayA.valueSchema(), arrayB.valueSchema());
    }

    private static boolean structEquals(final Schema structA, final Schema structB) {
      return structA.fields().isEmpty()
          || structB.fields().isEmpty()
          || Objects.equals(structA.fields(), structB.fields());
    }

    @Override
    public String toString() {
      return FORMATTER.format(schema) + (isVararg ? "(VARARG)" : "");
    }
  }

}
