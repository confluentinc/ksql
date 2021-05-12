/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.expression.tree;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.parser.NodeLocation;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

public class LambdaFunctionCallTest {

    public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
    public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);
    private static final List<String> SOME_ARGUMENTS = ImmutableList.of("X", "Y");
    private static final List<String> OTHER_ARGUMENTS = ImmutableList.of("X");
    private static final Expression SOME_EXPRESSION = new StringLiteral("steven");
    private static final Expression OTHER_EXPRESSION = new StringLiteral("steve");

    @Test
    public void shouldImplementHashCodeAndEqualsProperty() {
        new EqualsTester()
        .addEqualityGroup(
            // Note: At the moment location does not take part in equality testing
            new LambdaFunctionCall(SOME_ARGUMENTS, SOME_EXPRESSION),
            new LambdaFunctionCall(SOME_ARGUMENTS, SOME_EXPRESSION),
            new LambdaFunctionCall(Optional.of(SOME_LOCATION), SOME_ARGUMENTS, SOME_EXPRESSION),
            new LambdaFunctionCall(Optional.of(OTHER_LOCATION), SOME_ARGUMENTS, SOME_EXPRESSION)
        )
        .addEqualityGroup(
            new LambdaFunctionCall(SOME_ARGUMENTS, OTHER_EXPRESSION),
            new LambdaFunctionCall(SOME_ARGUMENTS, OTHER_EXPRESSION),
            new LambdaFunctionCall(Optional.of(SOME_LOCATION), SOME_ARGUMENTS, OTHER_EXPRESSION),
            new LambdaFunctionCall(Optional.of(OTHER_LOCATION), SOME_ARGUMENTS, OTHER_EXPRESSION)
        ).addEqualityGroup(
            new LambdaFunctionCall(OTHER_ARGUMENTS, SOME_EXPRESSION),
            new LambdaFunctionCall(OTHER_ARGUMENTS, SOME_EXPRESSION),
            new LambdaFunctionCall(Optional.of(SOME_LOCATION), OTHER_ARGUMENTS, SOME_EXPRESSION),
            new LambdaFunctionCall(Optional.of(OTHER_LOCATION), OTHER_ARGUMENTS, SOME_EXPRESSION)
        ).addEqualityGroup(
            new LambdaFunctionCall(OTHER_ARGUMENTS, OTHER_EXPRESSION),
            new LambdaFunctionCall(OTHER_ARGUMENTS, OTHER_EXPRESSION),
            new LambdaFunctionCall(Optional.of(SOME_LOCATION), OTHER_ARGUMENTS, OTHER_EXPRESSION),
            new LambdaFunctionCall(Optional.of(OTHER_LOCATION), OTHER_ARGUMENTS, OTHER_EXPRESSION)
        )
        .testEquals();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnDuplicateArguments() {
        new LambdaFunctionCall(ImmutableList.of("X", "X", "Y"), SOME_EXPRESSION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnNoArguments() {
        new LambdaFunctionCall(ImmutableList.of(), SOME_EXPRESSION);
    }
}