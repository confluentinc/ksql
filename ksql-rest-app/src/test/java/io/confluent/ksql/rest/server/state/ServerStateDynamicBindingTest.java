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

package io.confluent.ksql.rest.server.state;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import io.confluent.ksql.rest.server.resources.KsqlResource;
import java.lang.reflect.Method;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.FeatureContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class ServerStateDynamicBindingTest {
  @Mock
  private FeatureContext featureContext;
  @Mock
  private ServerState serverState;
  private ServerStateDynamicBinding binding;

  @Before
  public void setuo() {
    binding = new ServerStateDynamicBinding(serverState);
  }

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Test
  public void shouldBindFilterForKSQLResources() {
    // Given:
    final ResourceInfo resourceInfo = new ResourceInfo() {
      @Override
      public Method getResourceMethod() {
        return null;
      }

      @Override
      public Class<?> getResourceClass() {
        return KsqlResource.class;
      }
    };

    // When:
    binding.configure(resourceInfo, featureContext);

    // Then:
    verify(featureContext).register(any(ServerStateFilter.class));
  }

  @Test
  public void shouldNotBindFilterForPluggedResources() {
    // Given:
    final ResourceInfo resourceInfo = new ResourceInfo() {
      @Override
      public Method getResourceMethod() {
        return null;
      }

      @Override
      public Class<?> getResourceClass() {
        return String.class;
      }
    };

    // When:
    binding.configure(resourceInfo, featureContext);

    // Then:
    verifyZeroInteractions(featureContext);
  }
}