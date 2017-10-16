/**
 * Copyright 2017 Confluent Inc.
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
 **/

package io.confluent.ksql.util;

import javax.annotation.Nullable;

public class KsqlPreconditions {

  public static <T> T checkNotNull(T reference, @Nullable Object errorMessage) {
    if (reference == null) {
      throw new KsqlException(String.valueOf(errorMessage));
    } else {
      return reference;
    }
  }

  public static void checkArgument(boolean expression, @Nullable Object errorMessage) {
    if (!expression) {
      throw new KsqlException(String.valueOf(errorMessage));
    }
  }

}