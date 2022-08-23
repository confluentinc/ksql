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
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.GenericType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
 *   <li>If only one exact method exists, return it.</li>
 *   <li>If a method exists such that all non-null {@code Schema} in the
 *       parameters match, and all of the method's other parameters are null,
 *       return that method.</li>
 *   <li>If two methods exist that match given the above rules, and one
 *       does not have variable arguments (e.g. {@code String...}, return
 *       that one.</li>
 *   <li>If two methods exist that match given the above rules, and both
 *       have variable arguments, return the method with the more non-variable
 *       arguments.</li>
 *   <li>If two methods exist that match given the above rules, return the
 *       method with fewer generic arguments, including any
 *       {@code Object...} arguments.</li>
 *   <li>If two methods exist that match given the above rules, return the
 *       method without an {@code Object...} argument.</li>
 *   <li>If two methods exist that match given the above rules, return the
 *       method with the variadic argument in the later position.</li>
 *   <li>If two methods exist that match given the above rules, the function
 *       call is ambiguous and an exception is thrown.</li>
 * </ul>
 */
public class UdfIndex<T extends FunctionSignature> {
  // this class is implemented as a custom Trie data structure that resolves
  // the rules described above. the Trie is built so that each node in the
  // trie references a single possible pair of parameters in the signature.
  // take for example the following method signatures:
  //
  // A: void foo(String a)
  // B: void foo(String a, Integer b)
  // C: void foo(String a, Integer... ints)
  // D: void foo(Integer a)
  //
  // The final constructed trie would look like:
  //
  // D <-- (int, ?) -- Ø
  //                 /   \
  //                /     \
  //   A <-- (str, ?)     (str, int) --> B
  //                       /       \
  //                      /        (int, ?) --> C
  //                     /
  //                   (int, int) (VARARGS) --> C
  //                   /
  //                 (int, ?) --> C
  //
  // To resolve a query against the trie, this class simply traverses down each
  // matching path of the trie until it exhausts the input List<Schema>. The
  // List<Schema> is consumed at both ends, one parameter at a time, to make a
  // pair. The final node that it reaches will contain the method that is returned.
  // If multiple methods are returned, the rules described above are used to select
  // the best candidate (e.g. foo(null, int) matches B and C).
  //
  // See also:
  // Tree diagram: Docs: https://docs.google.com/document/d/14cfKl6A8HGM4zwnGvwuCMaJiGWKH4yUNdysEPX-Wy1Y/edit?usp=sharing
  //               Gist: https://gist.github.com/reneesoika/2ec934940c98dad6b0f68c89769fbe42

  private static final Logger LOG = LoggerFactory.getLogger(UdfIndex.class);
  private static final ParamType OBJ_VAR_ARG = ArrayType.of(ParamTypes.ANY);

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
    final List<ParameterInfo> parameters = function.parameterInfo();
    if (allFunctions.put(function.parameters(), function) != null) {
      throw new KsqlFunctionException(
          "Can't add function " + function.name()
              + " with parameters " + function.parameters()
              + " as a function with the same name and parameter types already exists "
              + allFunctions.get(function.parameters())
      );
    }

    /* Build the tree for non-variadic functions, or build the tree that includes one variadic
    argument for variadic functions. */
    final Pair<Node, Integer> variadicParentAndOffset = buildTree(
            root,
            parameters,
            function,
            false,
            false
    );
    final Node variadicParent = variadicParentAndOffset.getLeft();

    if (variadicParent != null) {
      final int offset = variadicParentAndOffset.getRight();

      // Build a branch of the tree that handles var args given as list.
      buildTree(variadicParent, parameters, function, true, false);

      // Determine which side the variadic parameter is on.
      final boolean isLeftVariadic = parameters.get(offset).isVariadic();

      /* Build a branch of the tree that handles no var args by excluding the variadic param.
      Note that non-variadic parameters that are always paired with another non-variadic parameter
      are not included in paramsWithoutVariadic. For example, if the function signature is
      (int, boolean..., double, bigint, decimal), then paramsWithoutVariadic will be
      [double, bigint]. The (int, decimal) edge will be in a node above the variadicParent, so
      we need to only include parameters that might be paired with a variadic parameter. */
      final List<ParameterInfo> paramsWithoutVariadic = isLeftVariadic
              ? parameters.subList(offset + 1, parameters.size() - offset)
              : parameters.subList(offset, parameters.size() - offset - 1);
      buildTree(
              variadicParent,
              paramsWithoutVariadic,
              function,
              false,
              false
      );

      // Build branches of the tree that handles more than two var args.
      final ParameterInfo variadicParam = isLeftVariadic
              ? parameters.get(offset)
              : parameters.get(parameters.size() - offset - 1);

      // Create copies of the variadic parameter that will be used to build the tree.
      final int maxAddedVariadics = paramsWithoutVariadic.size() + 1;
      final List<ParameterInfo> addedVariadics = Collections.nCopies(
              maxAddedVariadics,
              variadicParam
      );

      // Add the copies of the variadic parameter on the same side as the variadic parameter.
      final List<ParameterInfo> combinedAllParams = new ArrayList<>();
      int fromIndex;
      int toIndex;
      if (isLeftVariadic) {
        combinedAllParams.addAll(addedVariadics);
        combinedAllParams.addAll(paramsWithoutVariadic);
        fromIndex = maxAddedVariadics - 1;
        toIndex = combinedAllParams.size();
      } else {
        combinedAllParams.addAll(paramsWithoutVariadic);
        combinedAllParams.addAll(addedVariadics);
        fromIndex = 0;
        toIndex = combinedAllParams.size() - maxAddedVariadics + 1;
      }

      /* Successively build branches of the tree that include one additional variadic parameter
      until maxAddedVariadics have been processed. During the first iteration, buildTree()
      iterates through the already-created branch for the case where there is one variadic
      parameter. This is necessary because the variadic loop was not added when that branch
      was built, but it needs to be added if there are no non-variadic parameters (i.e.
      paramsWithoutVariadic.size() == 0 and maxAddedVariadics == 1).

      The number of nodes added here is quadratic with respect to paramsWithoutVariadic.size().
      However, this tree is only built on ksql startup, and this tree structure allows resolving
      functions with variadic arguments in the middle in linear time. The number of node generated
      as a function of paramsWithoutVariadic.size() is roughly 0.25x^2 + 1.5x + 3, which is not
      excessive for reasonable numbers of arguments. */
      while (fromIndex >= 0 && toIndex <= combinedAllParams.size()) {
        buildTree(
                variadicParent,
                combinedAllParams.subList(fromIndex, toIndex),
                function,
                false,

                /* Add the variadic loop after longest branch to handle variadic parameters not
                paired with a non-variadic parameter. */
                toIndex - fromIndex == combinedAllParams.size()

        );

        // Increment the size of the sublist in the direction of the variadic parameters.
        if (isLeftVariadic) {
          fromIndex--;
        } else {
          toIndex++;
        }

      }

    }

  }

  T getFunction(final List<SqlArgument> arguments) {

    // first try to get the candidates without any implicit casting
    Optional<T> candidate = findMatchingCandidate(arguments, false);
    if (candidate.isPresent()) {
      return candidate.get();
    } else if (!supportsImplicitCasts) {
      throw createNoMatchingFunctionException(arguments);
    }

    // if none were found (candidate isn't present) try again with implicit casting
    candidate = findMatchingCandidate(arguments, true);
    if (candidate.isPresent()) {
      return candidate.get();
    }
    throw createNoMatchingFunctionException(arguments);
  }

  /* The given node will be modified so that it is the root of a tree that can resolve a function
  with the given parameters. */
  private Pair<Node, Integer> buildTree(final Node root, final List<ParameterInfo> parameters,
                                        final T function, final boolean keepArrays,
                                        final boolean appendVariadicLoop) {
    final int rightStartIndex = parameters.size() - 1;

    Node curr = root;
    Node parent = curr;

    // The edge containing the first variadic parameterInfo starts at the parentOfVariadic.
    Node parentOfVariadic = null;
    int variadicOffset = -1;

    for (int offset = 0; offset < indexAfterCenter(parameters.size()); offset++) {
      final ParameterInfo leftParamInfo = parameters.get(offset);
      final int rightParamIndex = rightStartIndex - offset;
      final ParameterInfo rightParamInfo = parameters.get(rightParamIndex);

      final Parameter leftParam = new Parameter(
              toType(leftParamInfo, keepArrays),
              false
      );
      final Parameter rightParam = offset == rightParamIndex
              ? null
              : new Parameter(toType(rightParamInfo, keepArrays), false);

      parent = curr;
      curr = curr.children.computeIfAbsent(Pair.of(leftParam, rightParam), ignored -> new Node());

      // The case of one var arg will be handled here, but we need to handle the other cases later
      if (leftParamInfo.isVariadic() || rightParamInfo.isVariadic()) {
        parentOfVariadic = parent;
        variadicOffset = offset;
      }
    }

    if (appendVariadicLoop) {

      /* Sanity check--throw a clearer exception if we aren't in a valid state
      since parameters.get() will fail anyway. */
      if (variadicOffset < 0) {
        throw new IllegalStateException(
                String.format(
                        "appendVariadicLoop was set for a function named %s "
                          + "with parameters %s. The actual parameter types "
                          + "being used to build the tree were %s.",
                        function.name(),
                        function.parameterInfo(),
                        parameters
                )
        );
      }

      /* Setting isVararg to true makes the Parameter have a different hash, so it won't conflict
      with any non-variadic functions added later. */
      final ParamType varArgSchema = toType(parameters.get(variadicOffset), keepArrays);
      final Parameter varArg = new Parameter(varArgSchema, true);

      /* Add a self-referential loop for varargs so that we can add as many of the same param
      at the end and still retrieve this node. */
      final Node loop = parent.children.computeIfAbsent(
              Pair.of(varArg, varArg),
              ignored -> new Node()
      );
      loop.update(function);
      loop.children.putIfAbsent(Pair.of(varArg, varArg), loop);

      // Add a leaf node to handle an odd number of arguments after the loop
      final Node leaf = loop.children.computeIfAbsent(
              Pair.of(varArg, null),
              ignored -> new Node()
      );
      leaf.update(function);

    }

    curr.update(function);

    return Pair.of(parentOfVariadic, variadicOffset);
  }

  private int indexAfterCenter(final int size) {
    return (size + 1) / 2;
  }

  private ParamType toType(final ParameterInfo info, final boolean keepArrays) {
    return info.isVariadic() && !keepArrays
            ? ((ArrayType) info.type()).element()
            : info.type();
  }

  private Optional<T> findMatchingCandidate(
      final List<SqlArgument> arguments, final boolean allowCasts) {

    final List<Node> candidates = new ArrayList<>();

    getCandidates(arguments, 0, root, candidates, new HashMap<>(), allowCasts);
    candidates.sort(Node::compare);

    final int len = candidates.size();
    if (len == 1) {
      return Optional.of(candidates.get(0).value);
    } else if (len > 1) {
      if (candidates.get(len - 1).compare(candidates.get(len - 2)) > 0) {
        return Optional.of(candidates.get(len - 1).value);
      }
      throw createVagueImplicitCastException(arguments);
    }

    return Optional.empty();
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private void getCandidates(
      final List<SqlArgument> arguments,
      final int argOffset,
      final Node current,
      final List<Node> candidates,
      final Map<GenericType, SqlType> reservedGenerics,
      final boolean allowCasts
  ) {
    final int rightArgIndex = arguments.size() - 1 - argOffset;

    // The left and right "pointers" have passed each other, so we must have processed all args.
    if (argOffset > rightArgIndex) {
      if (current.value != null) {
        candidates.add(current);
      }
      return;
    }

    final SqlArgument leftArg = arguments.get(argOffset);

    // We can't simply check if the right arg is null because the user may have provided null.
    final boolean isRightArgEmpty = rightArgIndex == argOffset;
    final SqlArgument rightArg = isRightArgEmpty ? null : arguments.get(rightArgIndex);

    for (final Entry<Pair<Parameter, Parameter>, Node> candidate : current.children.entrySet()) {
      final Map<GenericType, SqlType> reservedCopy = new HashMap<>(reservedGenerics);

      /* If there is an odd number of parameters, we always treat the single argument as the
      left argument, so we do not need to check for null here. */
      final Parameter leftParam = candidate.getKey().getLeft();
      final boolean leftParamAccepts = leftParam.accepts(leftArg, reservedCopy, allowCasts);

      // The right argument might be empty, so we do need to check for empty on the right side.
      final Parameter rightParam = candidate.getKey().getRight();
      final boolean rightParamAlsoEmpty = rightParam == null && isRightArgEmpty;
      final boolean rightParamAccepts = !isRightArgEmpty && rightParam != null
              && rightParam.accepts(rightArg, reservedCopy, allowCasts);

      if (leftParamAccepts && (rightParamAlsoEmpty || rightParamAccepts)) {
        final Node node = candidate.getValue();
        getCandidates(arguments, argOffset + 1, node, candidates, reservedCopy, allowCasts);
      }
    }
  }

  private String getParamsAsString(final List<SqlArgument> paramTypes) {
    return paramTypes.stream()
        .map(argument -> {
          if (argument == null) {
            return "null";
          } else {
            return argument.toString(FormatOptions.noEscape());
          }
        })
        .collect(Collectors.joining(", ", "(", ")"));
  }

  private String getAcceptedTypesAsString() {
    return allFunctions.values().stream()
        .map(UdfIndex::formatAvailableSignatures)
        .collect(Collectors.joining(System.lineSeparator()));
  }

  private KsqlException createVagueImplicitCastException(final List<SqlArgument> paramTypes) {
    LOG.debug("Current UdfIndex:\n{}", describe());
    throw new KsqlException("Function '" + udfName
        + "' cannot be resolved due to ambiguous method parameters "
        + getParamsAsString(paramTypes) + "."
        + System.lineSeparator()
        + "Use CAST() to explicitly cast your parameters to one of the supported function calls."
        + System.lineSeparator()
        + "Valid function calls are:"
        + System.lineSeparator()
        + getAcceptedTypesAsString()
        + System.lineSeparator()
        + "For detailed information on a function run: DESCRIBE FUNCTION <Function-Name>;");
  }

  private KsqlException createNoMatchingFunctionException(final List<SqlArgument> paramTypes) {
    LOG.debug("Current UdfIndex:\n{}", describe());

    final String requiredTypes = getParamsAsString(paramTypes);

    final String acceptedTypes = getAcceptedTypesAsString();

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
    final List<ParameterInfo> parameters = function.parameterInfo();

    final StringBuilder result = new StringBuilder();
    result.append(function.name().toString(FormatOptions.noEscape())).append("(");

    for (int i = 0; i < parameters.size(); i++) {
      final ParameterInfo param = parameters.get(i);
      final String type = param.isVariadic()
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
                .thenComparing(fun -> -countGenerics(fun))
                .thenComparing(fun ->
                        fun.parameters().stream().anyMatch(
                                (param) -> param.equals(OBJ_VAR_ARG)
                        )
                        ? 0 : 1
                ).thenComparing(this::indexOfVariadic)
        );

    private final Map<Pair<Parameter, Parameter>, Node> children;
    private T value;

    private Node() {
      this.children = new HashMap<>();
      this.value = null;
    }

    private void update(final T function) {
      final int compareVal = compareFunctions.compare(function, value);
      if (compareVal > 0) {
        value = function;
      }
    }

    private void describe(final StringBuilder builder, final int indent) {
      for (final Entry<Pair<Parameter, Parameter>, Node> child : children.entrySet()) {
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
      return compareFunctions.compare(value, other.value);
    }

    private int countGenerics(final T function) {
      return function.parameters().stream()
          .filter((param) -> GenericsUtil.hasGenerics(param)
                  || param.equals(OBJ_VAR_ARG))
          .mapToInt(p -> 1)
          .sum();
    }

    private int indexOfVariadic(final T function) {
      if (function == null) {
        return -1;
      }

      return IntStream.range(0, function.parameterInfo().size())
              .filter((index) -> function.parameterInfo().get(index).isVariadic())
              .findFirst()
              .orElse(-1);
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
      this.type = type;
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
