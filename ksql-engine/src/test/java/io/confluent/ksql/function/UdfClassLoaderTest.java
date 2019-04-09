/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.io.File;
import java.nio.file.Path;
import org.junit.Test;

public class UdfClassLoaderTest {

  private static final String ONLY_IN_JAR = "org.damian.ksql.udf.ToString";
  private static final String ONLY_IN_PARENT = "class.that.exists.only.in.parent";
  private static final String IN_PARENT_AND_JAR = "org.damian.ksql.udf.ToStruct";

  // The jar contains the class org.damian.ksql.udf.ToString and org.damian.ksql.udf.ToStruct.
  private final Path udfJar = new File("src/test/resources/udf-example.jar").toPath();
  private final MyClassLoader parentLoader = new MyClassLoader();
  private final UdfClassLoader udfClassLoader =
      UdfClassLoader.newClassLoader(udfJar, parentLoader, resourceName -> false);

  @Test
  public void shouldLoadClassesInPath() throws ClassNotFoundException {
    final UdfClassLoader udfClassLoader = UdfClassLoader.newClassLoader(udfJar,
        getClass().getClassLoader(), resourceName -> false);

    assertThat(udfClassLoader.loadClass(ONLY_IN_JAR, true), not(nullValue()));
  }

  @Test(expected = ClassNotFoundException.class)
  public void shouldThrowClassNotFoundIfClassIsBlacklisted() throws ClassNotFoundException {
    final UdfClassLoader udfClassLoader = UdfClassLoader.newClassLoader(udfJar,
        getClass().getClassLoader(), resourceName -> true);
    udfClassLoader.loadClass(ONLY_IN_JAR, true);
  }

  @Test
  public void shouldLoadClassesFromParentIfNotFoundInChild() throws ClassNotFoundException {
    assertThat(udfClassLoader.loadClass(ONLY_IN_PARENT, true), equalTo(OnlyInParent.class));
  }

  @Test
  public void shouldLoadNonConfluentClassesFromChildFirst() throws ClassNotFoundException {
    assertThat(parentLoader.findClass(IN_PARENT_AND_JAR), is(equalTo(InParentAndJar.class)));
    assertThat(udfClassLoader.loadClass(IN_PARENT_AND_JAR, true), not(InParentAndJar.class));
  }

  private class MyClassLoader extends ClassLoader {
    @Override
    public Class<?> findClass(String name) throws ClassNotFoundException {
      switch (name) {
        case ONLY_IN_PARENT: return OnlyInParent.class;
        case IN_PARENT_AND_JAR: return InParentAndJar.class;
        default: throw new ClassNotFoundException(name);
      }
    }
  }

  private static class OnlyInParent {}
  private static class InParentAndJar {}
}
