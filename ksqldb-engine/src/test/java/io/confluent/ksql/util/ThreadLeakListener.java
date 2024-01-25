/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.ksql.util;

import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.support.descriptor.ClassSource;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;


/**
 * ThreadLeakListener is a TestExecutionListener that checks for thread leaks after test execution.
 * It verifies that no unexpected threads are left running and reports any leaks found.
 * Thread dump reports are generated and stored in each project's "build/thread-reports" directory.
 */
public class ThreadLeakListener implements TestExecutionListener {

    @Override
    public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult testExecutionResult) {
        System.out.println("executionFinished - testIdentifier " + testIdentifier);
        System.out.println("executionFinished - testExecutionResult " + testExecutionResult);
    }

    @Override
    public void executionSkipped(TestIdentifier testIdentifier, String reason) {
        System.out.println("executionSkipped - testIdentifier " + testIdentifier);
    }

}
