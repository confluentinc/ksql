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

import org.junit.Ignore;
import org.junit.Test;

/**
 * This test exists only to be able to generate topologies as part of the release process
 * It can be run manually from the IDE
 * It is deliberately excluded from the test suite
 */
@Ignore
public final class ManualTopologyFileGeneratorTest {

    @Test
    public void manuallyGenerateTopologies() throws Exception {
        TopologyFileGenerator.generateTopologies();
    }
}
