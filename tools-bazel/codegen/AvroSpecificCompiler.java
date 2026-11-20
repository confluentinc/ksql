/*
 * Copyright 2026 Confluent Inc.
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

package tools.bazel.codegen;

import java.io.File;
import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.generic.GenericData;

/**
 * Bazel stand-in for avro-maven-plugin's {@code schema} goal: compiles .avsc
 * files to Java with the same SpecificCompiler settings ksql's pom uses
 * ({@code stringType=String}, everything else at the plugin defaults).
 *
 * <p>Usage: {@code AvroSpecificCompiler <outDir> <schema.avsc>...}
 */
public final class AvroSpecificCompiler {

  private AvroSpecificCompiler() {
  }

  public static void main(final String[] args) throws Exception {
    final File out = new File(args[0]);
    final Schema.Parser parser = new Schema.Parser();
    for (int i = 1; i < args.length; i++) {
      final File src = new File(args[i]);
      final SpecificCompiler compiler = new SpecificCompiler(parser.parse(src));
      compiler.setStringType(GenericData.StringType.String);
      compiler.setOutputCharacterEncoding("UTF-8");
      compiler.compileToDestination(src, out);
    }
  }
}
