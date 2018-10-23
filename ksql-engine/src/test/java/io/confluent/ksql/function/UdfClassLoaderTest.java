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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;

import java.io.File;
import java.nio.file.Path;
import org.junit.Test;

public class UdfClassLoaderTest {

  /*
    The jar contains the class org.damian.ksql.udf.ToString. It also contains the
    contents the classes from slf4j-log4j12-1.7.25.jar
   */
  private final Path udfJar = new File("src/test/resources/udf-example.jar").toPath();
  private final UdfClassLoader udfClassLoader = UdfClassLoader.newClassLoader(udfJar,
      getClass().getClassLoader(), resourceName -> false);


  @Test
  public void shouldLoadClassesInPath() throws ClassNotFoundException {
    final UdfClassLoader udfClassLoader = UdfClassLoader.newClassLoader(udfJar,
        getClass().getClassLoader(), resourceName -> false);

    assertThat(udfClassLoader.loadClass("org.damian.ksql.udf.ToString", true), not(nullValue()));
  }

  @Test(expected = ClassNotFoundException.class)
  public void shouldThrowClassNotFoundIfClassIsBlacklisted() throws ClassNotFoundException {
    final UdfClassLoader udfClassLoader = UdfClassLoader.newClassLoader(udfJar,
        getClass().getClassLoader(), resourceName -> true);
    udfClassLoader.loadClass("org.damian.ksql.udf.ToString", true);
  }

  @Test
  public void shouldLoadClassesFromParentIfNotFoundInChild() throws ClassNotFoundException {
    assertThat(udfClassLoader.loadClass("io.confluent.ksql.function.UdfClassLoaderTest", true),
        equalTo(UdfClassLoaderTest.class));

  }

  @Test
  public void shouldLoadNonConfluentClassesFromChildFirst() throws ClassNotFoundException {
    assertThat(udfClassLoader.loadClass("org.slf4j.impl.Log4jLoggerAdapter", true),
        not(org.slf4j.impl.Log4jLoggerAdapter.class));
  }

}