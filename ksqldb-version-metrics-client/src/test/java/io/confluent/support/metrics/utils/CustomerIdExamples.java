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

package io.confluent.support.metrics.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CustomerIdExamples {

  public static final List<String> VALID_CUSTOMER_IDS = Collections.unmodifiableList(Arrays.asList(
      "C0", "c1", "C1", "c12", "C22", "c123", "C333", "c1234", "C4444",
      "C00000", "C12345", "C99999", "C123456789", "C123456789012345678901234567890",
      "c00000", "c12345", "c99999", "c123456789", "c123456789012345678901234567890"
  ));

  public static final List<String> VALID_CASE_SENSISTIVE_NEW_CUSTOMER_IDS =
      Collections.unmodifiableList(Arrays.asList(
          "5003001200D8cyJ",
          "abbcaabcaaDwcyJ",
          "abbcaabcaadwcyj",
          "abbcaAbcAadwcyj",
          "AGGQAAFLSSDWPYJ",
          "AGGQAAfLSSDWpYJ",
          "123456789012345",
          "c23456789012345",
          "C23456789012345"
      ));

  public static final List<String> VALID_CASE_INSENSISTIVE_NEW_CUSTOMER_IDS =
      Collections.unmodifiableList(Arrays.asList(
          "123456789012345123",
          "123456789012345avc",
          "5003001200D8cyJaaa",
          "5003001200D8cyJ123",
          "abbcaabcaadwcyjsss",
          "AGGQAAFLSSDWPYJTTT"
      ));

  // These invalid customer ids should not include valid anonymous user IDs.
  public static final List<String> INVALID_CUSTOMER_IDS =
      Collections.unmodifiableList(Arrays.asList(
          "0c000", "0000C", null, "", "c", "C", "Hello", "World", "1", "12", "123", "1234", "12345",
          "5003001200D8cy",
          "abbcaabcaaDwcyJa",
          "abbcaabcaadwc*j",
          "AGGQAAFLSSD^PYJ",
          "123456789$12345",
          "12345678901234512",
          "123456789012345avca",
          "5003001200D8cyJaaa88",
          "5003001200D8cyJ1@3",
          "abbcaabcaadwcyj!ss",
          "AGGQAAFLSSDWPYJ_TT"
      ));

  public static final List<String> VALID_ANONYMOUS_IDS =
      Collections.unmodifiableList(Arrays.asList("anonymous", "ANONYMOUS", "anonyMOUS"));

  // These invalid anonymous user IDs should not include valid customer IDs.
  public static final List<String> INVALID_ANONYMOUS_IDS =
      Collections.unmodifiableList(Arrays.asList(null, "", "anon", "anonymou", "ANONYMOU"));

}
