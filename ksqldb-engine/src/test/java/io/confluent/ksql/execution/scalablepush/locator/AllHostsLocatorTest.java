package io.confluent.ksql.execution.scalablepush.locator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.scalablepush.locator.PushLocator.KsqlNode;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AllHostsLocatorTest {

  @Mock
  private PersistentQueryMetadata metadata1;
  @Mock
  private PersistentQueryMetadata metadata2;
  @Mock
  private StreamsMetadata streamsMetadata1;
  @Mock
  private StreamsMetadata streamsMetadata2;
  @Mock
  private StreamsMetadata streamsMetadata3;

  @Test
  public void shouldLocate() throws MalformedURLException {
    // Given:
    final AllHostsLocator locator = new AllHostsLocator(
        () -> ImmutableList.of(metadata1, metadata2),
        new URL("http://localhost:8088")
    );
    when(metadata1.getAllStreamsHostMetadata())
        .thenReturn(ImmutableList.of(streamsMetadata1, streamsMetadata2));
    when(metadata2.getAllStreamsHostMetadata())
        .thenReturn(ImmutableList.of(streamsMetadata3));
    when(streamsMetadata1.hostInfo())
        .thenReturn(new HostInfo("abc", 101), new HostInfo("localhost", 8088));
    when(streamsMetadata2.hostInfo()).thenReturn(new HostInfo("localhost", 8088));
    when(streamsMetadata3.hostInfo()).thenReturn(new HostInfo("localhost", 8089));

    // When:
    final List<KsqlNode> nodes = ImmutableList.copyOf(locator.locate());

    // Then:
    assertThat(nodes.size(), is(3));
    assertThat(nodes.get(0).isLocal(), is(false));
    assertThat(nodes.get(0).location().toString(), is("http://abc:101/"));
    assertThat(nodes.get(1).isLocal(), is(true));
    assertThat(nodes.get(1).location().toString(), is("http://localhost:8088/"));
    assertThat(nodes.get(2).isLocal(), is(false));
    assertThat(nodes.get(2).location().toString(), is("http://localhost:8089/"));
  }
}
