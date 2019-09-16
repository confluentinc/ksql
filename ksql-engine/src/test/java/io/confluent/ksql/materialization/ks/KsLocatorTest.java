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

package io.confluent.ksql.materialization.ks;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import io.confluent.ksql.materialization.Locator.KsqlNode;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsLocatorTest {

  private static final String STORE_NAME = "someStoreName";
  private static final URL LOCAL_HOST_URL = localHost();
  private static final Struct SOME_KEY = new Struct(SchemaBuilder.struct().build());

  @Mock
  private KafkaStreams kafkaStreams;
  @Mock
  private Serializer<Struct> keySerializer;
  @Mock
  private HostInfo hostInfo;

  private KsLocator locator;

  @Before
  public void setUp() {
    locator = new KsLocator(STORE_NAME, kafkaStreams, keySerializer, LOCAL_HOST_URL);

    givenOwnerMetadata(Optional.empty());

    when(hostInfo.host()).thenReturn("remoteHost");
    when(hostInfo.port()).thenReturn(2345);
  }

  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .setDefault(URL.class, LOCAL_HOST_URL)
        .setDefault(KafkaStreams.class, kafkaStreams)
        .setDefault(Serializer.class, keySerializer)
        .testConstructors(KsLocator.class, Visibility.PACKAGE);
  }

  @Test
  public void shouldRequestMetadata() {
    // When:
    locator.locate(SOME_KEY);

    // Then:
    verify(kafkaStreams).metadataForKey(STORE_NAME, SOME_KEY, keySerializer);
  }

  @Test
  public void shouldReturnEmptyIfOwnerNotKnown() {
    // Given:
    givenOwnerMetadata(Optional.empty());

    // When:
    final Optional<KsqlNode> result = locator.locate(SOME_KEY);

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldReturnOwnerIfKnown() {
    // Given:
    givenOwnerMetadata(Optional.of(hostInfo));

    // When:
    final Optional<KsqlNode> result = locator.locate(SOME_KEY);

    // Then:
    final Optional<URL> url = result.map(KsqlNode::location);
    assertThat(url.map(URL::getProtocol), is(Optional.of(LOCAL_HOST_URL.getProtocol())));
    assertThat(url.map(URL::getHost), is(Optional.of(hostInfo.host())));
    assertThat(url.map(URL::getPort), is(Optional.of(hostInfo.port())));
    assertThat(url.map(URL::getPath), is(Optional.of("/")));
  }

  @Test
  public void shouldReturnLocalOwnerIfSameAsSuppliedLocalHost() {
    // Given:
    when(hostInfo.host()).thenReturn(LOCAL_HOST_URL.getHost());
    when(hostInfo.port()).thenReturn(LOCAL_HOST_URL.getPort());
    givenOwnerMetadata(Optional.of(hostInfo));

    // When:
    final Optional<KsqlNode> result = locator.locate(SOME_KEY);

    // Then:
    assertThat(result.map(KsqlNode::isLocal), is(Optional.of(true)));
  }

  @Test
  public void shouldReturnLocalOwnerIfExplicitlyLocalHostOnSamePortAsSuppliedLocalHost() {
    // Given:
    when(hostInfo.host()).thenReturn("LocalHOST");
    when(hostInfo.port()).thenReturn(LOCAL_HOST_URL.getPort());
    givenOwnerMetadata(Optional.of(hostInfo));

    // When:
    final Optional<KsqlNode> result = locator.locate(SOME_KEY);

    // Then:
    assertThat(result.map(KsqlNode::isLocal), is(Optional.of(true)));
  }

  @Test
  public void shouldReturnRemoteOwnerForDifferentHost() {
    // Given:
    when(hostInfo.host()).thenReturn("different");
    when(hostInfo.port()).thenReturn(LOCAL_HOST_URL.getPort());
    givenOwnerMetadata(Optional.of(hostInfo));

    // When:
    final Optional<KsqlNode> result = locator.locate(SOME_KEY);

    // Then:
    assertThat(result.map(KsqlNode::isLocal), is(Optional.of(false)));
  }

  @Test
  public void shouldReturnRemoteOwnerForDifferentPort() {
    // Given:
    when(hostInfo.host()).thenReturn(LOCAL_HOST_URL.getHost());
    when(hostInfo.port()).thenReturn(LOCAL_HOST_URL.getPort() + 1);
    givenOwnerMetadata(Optional.of(hostInfo));

    // When:
    final Optional<KsqlNode> result = locator.locate(SOME_KEY);

    // Then:
    assertThat(result.map(KsqlNode::isLocal), is(Optional.of(false)));
  }

  @Test
  public void shouldReturnRemoteOwnerForDifferentPortOnLocalHost() {
    // Given:
    when(hostInfo.host()).thenReturn("LOCALhost");
    when(hostInfo.port()).thenReturn(LOCAL_HOST_URL.getPort() + 1);
    givenOwnerMetadata(Optional.of(hostInfo));

    // When:
    final Optional<KsqlNode> result = locator.locate(SOME_KEY);

    // Then:
    assertThat(result.map(KsqlNode::isLocal), is(Optional.of(false)));
  }

  @SuppressWarnings("unchecked")
  private void givenOwnerMetadata(final Optional<HostInfo> hostInfo) {
    final StreamsMetadata metadata = hostInfo
        .map(hi -> {
          final StreamsMetadata md = mock(StreamsMetadata.class);
          when(md.hostInfo()).thenReturn(hostInfo.get());
          return md;
        })
        .orElse(StreamsMetadata.NOT_AVAILABLE);

    when(kafkaStreams.metadataForKey(any(), any(), any(Serializer.class)))
        .thenReturn(metadata);
  }

  private static URL localHost() {
    try {
      return new URL("http://somehost:1234");
    } catch (MalformedURLException e) {
      throw new AssertionError("Failed to build URL", e);
    }
  }
}