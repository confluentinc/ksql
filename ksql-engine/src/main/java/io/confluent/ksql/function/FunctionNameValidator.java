/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.function;

import com.google.common.collect.ImmutableSet;

import java.util.Set;
import java.util.function.Predicate;

/**
 * Check that a function name is valid. It is valid if it is not a Java reserved word
 * and not a ksql reserved word and is a valid java identifier.
 */
class FunctionNameValidator implements Predicate<String> {
  private static final Set<String> JAVA_RESERVED_WORDS
      = ImmutableSet.<String>builder()
      .add("abstract").add("assert").add("boolean").add("break").add("byte").add("case")
      .add("catch").add("char").add("class").add("const").add("continue").add("default")
      .add("double").add("else").add("enum").add("extends").add("do").add("final").add("finally")
      .add("float").add("for").add("goto").add("if").add("int").add("implements").add("import")
      .add("instanceof").add("interface").add("long").add("native").add("new").add("package")
      .add("private").add("public").add("protected").add("return").add("this").add("throw")
      .add("throws").add("transient").add("try").add("short").add("static").add("strictfp")
      .add("super").add("switch").add("synchronized").add("void").add("volatile").add("while")
      .build();

  // This has been generated from SqlBase.tokens
  private static final Set<String> KSQL_RESERVED_WORDS
      = ImmutableSet.<String>builder()
      .add("add").add("advance").add("all").add("alter").add("analyze").add("and").add("any")
      .add("approximate").add("array").add("as").add("asc").add("asterisk").add("at")
      .add("backquoted_identifier").add("beginning").add("bernoulli").add("between")
      .add("binary_literal").add("bracketed_comment").add("by").add("call").add("case")
      .add("cast").add("catalog").add("catalogs").add("coalesce").add("column")
      .add("columns").add("commit").add("committed").add("confidence")
      .add("constraint").add("create").add("cross").add("cube").add("current").add("current_date")
      .add("current_time").add("current_timestamp").add("data").add("date").add("day").add("days")
      .add("deallocate").add("decimal_value").add("delete").add("delimiter").add("desc")
      .add("describe").add("digit_identifier").add("distinct").add("distributed").add("drop")
      .add("else").add("end").add("eq").add("escape").add("except").add("execute").add("exists")
      .add("explain").add("export").add("extended").add("extract").add("false").add("first")
      .add("following").add("for").add("format").add("from").add("full").add("functions")
      .add("grant").add("graphviz").add("group").add("grouping").add("gt").add("gte")
      .add("having").add("hopping").add("hour").add("hours").add("identifier").add("if")
      .add("in").add("inner").add("insert").add("integer").add("integer_value").add("intersect")
      .add("interval").add("into").add("is").add("isolation").add("join").add("last")
      .add("left").add("level").add("like").add("limit").add("list").add("load").add("localtime")
      .add("localtimestamp").add("logical").add("lt").add("lte").add("map").add("millisecond")
      .add("milliseconds").add("minus").add("minute").add("minutes").add("month").add("months")
      .add("natural").add("neq").add("nfc").add("nfd").add("nfkc").add("nfkd").add("no")
      .add("normalize").add("not").add("null").add("nullif").add("nulls").add("on")
      .add("only").add("option").add("or").add("order").add("ordinality").add("outer")
      .add("over").add("partition").add("partitions").add("percent").add("plus").add("poissonized")
      .add("position").add("preceding").add("prepare").add("print").add("privileges")
      .add("properties").add("public").add("queries").add("query").add("quoted_identifier")
      .add("range").add("read").add("recursive").add("register").add("registered").add("rename")
      .add("repeatable").add("replace").add("rescaled").add("reset").add("revoke")
      .add("right").add("rollback").add("rollup").add("row").add("rows").add("run").add("sample")
      .add("schemas").add("script").add("second").add("seconds").add("select").add("serializable")
      .add("session").add("set").add("sets").add("show").add("simple_comment").add("size")
      .add("slash").add("smallint").add("some").add("start").add("stratify").add("stream")
      .add("streams").add("string").add("struct").add("system")
      .add("table").add("tables").add("tablesample").add("terminate").add("text").add("then")
      .add("time").add("timestamp").add("timestamp_with_time_zone").add("time_with_time_zone")
      .add("tinyint").add("to").add("topic").add("topics").add("transaction").add("true")
      .add("try").add("try_cast").add("tumbling")
      .add("type").add("t__0").add("t__1").add("t__2").add("t__3").add("t__4").add("t__5")
      .add("t__6").add("t__7").add("t__8").add("unbounded").add("uncommitted").add("union")
      .add("unnest").add("unrecognized").add("unset").add("use").add("using").add("values")
      .add("view").add("when").add("where").add("window").add("with").add("work")
      .add("write").add("ws").add("year").add("years").add("zone")
      .build();

  @Override
  public boolean test(final String functionName) {
    if (functionName == null
        || functionName.trim().isEmpty()
        || JAVA_RESERVED_WORDS.contains(functionName.toLowerCase())
        || KSQL_RESERVED_WORDS.contains(functionName.toLowerCase())) {
      return false;
    }

    return isValidJavaIdentifier(functionName);

  }

  private boolean isValidJavaIdentifier(final String functionName) {
    final char [] characters = functionName.toCharArray();
    if (!Character.isJavaIdentifierStart((int)characters[0])) {
      return false;
    }

    for (int i = 1; i < characters.length; i++) {
      if (!Character.isJavaIdentifierPart((int)characters[i])) {
        return false;
      }
    }
    return true;
  }
}
