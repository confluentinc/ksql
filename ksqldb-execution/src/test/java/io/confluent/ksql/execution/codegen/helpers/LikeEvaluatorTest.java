/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.codegen.helpers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.util.Objects;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class LikeEvaluatorTest {

  private final String name;
  private final String val;
  private final String pattern;
  private final Character escape;

  @Parameters(name = "{0}")
  public static Object[][] data() {
    // name | val | pattern | escape
    return new Object[][]{
        {"nothing special",               "foo", "foo", null},
        {"nothing special [X]",           "bar", "foo", null},
        {"empty percents",                "foo", "%foo%", null},
        {"empty percents [X]",            "bar", "%foo%", null},
        {"percents",                      "barfoobaz", "%foo%", null},
        {"percents [X]",                  "barbarbar", "%foo%", null},
        {"percents one side",             "barfoo", "%foo", null},
        {"percents one side [X]",         "barbarbar", "%foo", null},
        {"percents other side",             "barfoo", "bar%", null},
        {"percents other side [X]",         "barbarbar", "foo%", null},
        {"percents in the middle",             "barfoo", "b%o", null},
        {"percents in the middle [X]",         "barbarbar", "b%o", null},
        {"multiple percents in the middle",      "barfoo", "b%r%o", null},
        {"multiple percents in the middle [X]",  "barbarbar", "b%r%o", null},
        {"multiple percents back to back",       "barfoo", "b%%%o", null},
        {"multiple percents back to back [X]",   "barbarbar", "b%%%o", null},
        {"underscore",                           "foo", "f_o", null},
        {"underscore [X]",                       "bar", "f_o", null},
        {"nothing special with regex chars",      ".^$\\*", ".^$\\*", null},
        {"nothing special with regex chars [X]",  "a^$\\", ".^$\\*", null},
        {"escape with normal char",               "f%o", "f!%o", '!'},
        {"escape with normal char [X]",           "foo", "f!%o", '!'},
        {"escape the escape char",                "f!o", "f!!o", '!'},
        {"escape the escape char [X]",            "foo", "f!!o", '!'},
        {"escape with special char",              "f%o", "f%%o", '%'},
        {"escape with special char [X]",          "foo", "f%%o", '%'},
        {"escape with backslash char",            "f%o", "f\\%o", '\\'},
        {"escape with backslash char [X]",        "foo", "f\\%o", '\\'}
    };
  }

  public LikeEvaluatorTest(final String name, final String val, final String pattern, final Character escape) {
    this.name = Objects.requireNonNull(name, "name");
    this.val = Objects.requireNonNull(val, "val");
    this.pattern = Objects.requireNonNull(pattern, "pattern");
    this.escape = escape;
  }

  @Test
  public void shouldMatch() {
    // Then:
    if (escape == null) {
      assertThat(LikeEvaluator.matches(val, pattern), not(name.contains("[X]")));
    } else {
      assertThat(LikeEvaluator.matches(val, pattern, escape), not(name.contains("[X]")));
    }
  }

}