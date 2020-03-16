/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.test;

import org.apache.kafka.test.IntegrationTest;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/**
 * Do not combine this with `TopologyFileGenerator` as mvn will ignore the tests as the class
 * does not end in `Test`.
 */
@Category(IntegrationTest.class)
public final class TopologyFileGeneratorTest {

    @ClassRule
    public static final TemporaryFolder TMP = new TemporaryFolder();

    @Test
    public void shouldGenerateTopologies() throws Exception {
        TopologyFileGenerator.generateTopologies(TMP.newFolder().toPath());
    }
}
