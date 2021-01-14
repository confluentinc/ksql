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
package io.confluent.ksql.engine.rewrite;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.KsqlException;
import org.junit.Test;

public class LambdaContextTest {
    @Test
    public void shouldThrowIfSourceDoesNotExist() {
        // Given:
        final LambdaContext context = new LambdaContext(ImmutableList.of("X"));

        // When:
        context.addLambdaArguments(ImmutableList.of("Z", "Y"));

        // Then:
        assertThat(context.getLambdaArguments(), is(ImmutableList.of("X", "Z", "Y")));
    }

    @Test
    public void shouldThrowIfLambdaArgumentAlreadyUsed() {
        // Given:
        final LambdaContext context = new LambdaContext(ImmutableList.of("X"));

        // When:
        final Exception e = assertThrows(
            KsqlException.class,
            () -> context.addLambdaArguments(ImmutableList.of("X", "Y"))
        );

        // Then:
        assertThat(e.getMessage(), containsString(
        "Duplicate lambda arguments are not allowed."));
    }
}