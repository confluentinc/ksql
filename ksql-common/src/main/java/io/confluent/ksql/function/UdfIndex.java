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
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

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
public class UdfIndex {
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
  // the best candidate (e.g. foo(null, int) matches both B and E).

  private final UdfMetadata metadata;
  private final Node root = new Node();
  private final Map<List<Schema>, KsqlFunction> allFunctions;

  UdfIndex(final UdfMetadata metadata) {
    this.metadata = metadata;
    allFunctions = new HashMap<>();
  }

  void addFunction(final KsqlFunction function) {
    final List<Schema> arguments = function.getArguments();
    if (allFunctions.put(function.getArguments(), function) != null) {
      throw new KsqlException(
          "Can't add function "
              + function
              + " as a function with the same name and argument type already exists "
              + allFunctions.get(arguments)
      );
    }

    final int order = allFunctions.size();

    Node curr = root;
    Node parent = curr;
    for (final Schema arg : arguments) {
      final FunctionParameter param = new FunctionParameter(arg, false);
      parent = curr;
      curr = curr.children.computeIfAbsent(param, ignored -> new Node());
    }

    if (function.isVarArgs()) {
      // first add the function to the parent to address the
      // case of empty varargs
      parent.update(function, order);

      // then add a new child node with the parameter value type
      // and add this function to that node
      final Schema varargSchema = Iterables.getLast(function.getArguments());
      final FunctionParameter vararg = new FunctionParameter(varargSchema, true);
      final Node leaf = parent.children.computeIfAbsent(vararg, ignored -> new Node());
      leaf.update(function, order);

      // add a self referential loop for varargs so that we can
      // add as many of the same param at the end and still retrieve
      // this node
      leaf.children.putIfAbsent(vararg, leaf);
    }

    curr.update(function, order);
  }

  KsqlFunction getFunction(final List<Schema> parameters) {
    final List<Node> candidates = new ArrayList<>();
    getCandidates(parameters, 0, root, candidates);

    return candidates
        .stream()
        .max(Node::compare)
        .map(node -> node.value)
        .orElseThrow(() -> createNoMatchingFunctionException(parameters));
  }

  private void getCandidates(
      final List<Schema> parameters,
      final int paramIndex,
      final Node current,
      final List<Node> candidates
  ) {
    if (paramIndex == parameters.size()) {
      if (current.value != null) {
        candidates.add(current);
      }
      return;
    }

    current.children
        .entrySet()
        .stream()
        .filter(entry -> entry.getKey().matches(parameters.get(paramIndex)))
        .map(Entry::getValue)
        .forEach(node -> getCandidates(parameters, paramIndex + 1, node, candidates));
  }

  private KsqlException createNoMatchingFunctionException(final List<Schema> paramTypes) {
    final String sqlParamTypes = paramTypes.stream()
        .map(schema -> schema == null
            ? null
            : SchemaUtil.getSchemaTypeAsSqlType(schema.type()))
        .collect(Collectors.joining(", ", "[", "]"));

    return new KsqlException("Function '" + metadata.getName()
        + "' does not accept parameters of types:" + sqlParamTypes);
  }

  public Collection<KsqlFunction> values() {
    return allFunctions.values();
  }

  @SuppressWarnings("unused")
  @VisibleForTesting
  String describe() {
    final StringBuilder sb = new StringBuilder();
    sb.append("-ROOT\n");
    root.describe(sb, 1);
    return sb.toString();
  }

  private static final class Node {

    private static final Comparator<KsqlFunction> COMPARE_FUNCTIONS =
        Comparator.nullsFirst(
            Comparator
                .<KsqlFunction, Integer>comparing(fun -> fun.isVarArgs() ? 0 : 1)
                .thenComparing(fun -> fun.getArguments().size())
        );

    private final Map<FunctionParameter, Node> children;
    private KsqlFunction value;
    private int order = 0;

    private Node() {
      this.children = new HashMap<>();
      this.value = null;
    }

    private void update(final KsqlFunction function, final int order) {
      if (COMPARE_FUNCTIONS.compare(function, value) > 0) {
        value = function;
        this.order = order;
      }
    }

    private void describe(final StringBuilder builder, final int indent) {
      for (final Entry<FunctionParameter, Node> child : children.entrySet()) {
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

    public static int compare(final Node a, final Node b) {
      final int compare = COMPARE_FUNCTIONS.compare(a.value, b.value);
      return compare == 0 ? -(a.order - b.order) : compare;
    }
  }

  private static final class FunctionParameter  {
    private final Schema schema;
    private final boolean isOptional;
    private final boolean isVararg;

    private FunctionParameter(final Schema schema, final boolean isVararg) {
      this.schema = (schema instanceof SchemaBuilder) ? ((SchemaBuilder) schema).build() : schema;
      this.isOptional = schema.isOptional();
      this.isVararg = isVararg;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final FunctionParameter that = (FunctionParameter) o;
      // isOptional is excluded from equals and hashCode so that
      // primitive types will match their boxed counterparts. i.e,
      // primitive types are not optional, i.e., they don't accept null.
      return schema == that.schema && isVararg == that.isVararg;
    }

    @Override
    public int hashCode() {
      // isOptional is excluded from equals and hashCode so that
      // primitive types will match their boxed counterparts. i.e,
      // primitive types are not optional, i.e., they don't accept null.
      return Objects.hash(schema, isVararg);
    }

    public boolean matches(final Schema schema) {
      if (schema == null) {
        return isOptional || (isVararg && this.schema.valueSchema().isOptional());
      } else if (isVararg) {
        return this.schema.valueSchema().type().equals(schema.type());
      } else {
        return this.schema.type().equals(schema.type());
      }
    }

    @Override
    public String toString() {
      return SchemaUtil.getSqlTypeName(isVararg ? schema.valueSchema() : schema)
          + (isVararg ? "(VARARG)" : "");
    }
  }

}
