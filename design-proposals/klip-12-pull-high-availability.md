# KLIP 12 - Implement High-Availability for Pull queries

**Author**: Vicky Papavasileiou, Vinoth Chandar | 
**Release Target**: 5.5 | 
**Status**: _In Discussion_
**Discussion**: _link to the design discussion PR_

**tl;dr:** Enables high-availability for Ksql pull queries, in case of server failures. Current 
design for handling failure of an active ksql server, incurs an unavailability period of several 
seconds (10+) to few minutes due to the Streams rebalancing procedure. This work is based on new 
Kafka Streams APIs, introduced as a part of [KIP-535](https://cwiki.apache.org/confluence/display/KAFKA/KIP-535%3A+Allow+state+stores+to+serve+stale+reads+during+rebalance), 
which allow serving stale data from standby replicas to achieve high availability, while providing 
eventual consistency. 

## Motivation and background
Stateful persistent queries persist their state in state stores. These state stores are partitioned
and replicated across a number of standbys for faster recovery in case of 
[failures](https://docs.confluent.io/current/streams/architecture.html#fault-tolerance).
A KSQL server may host partitions from multiple state stores serving as the active host for some partitions
and the standby for others. Without loss of generality, we will focus on one partition and one state 
store for the remainder of this document. The active KSQL server is the server that hosts the 
active partition whereas the standby servers are the ones that host the replicas.  

Assume we have a load balancer (LB) and a cluster of three KSQL servers (`A`, `B`, `C`) where `A` 
is the active server and `B`, `C` are the standby replicas. `A` receives updates from the source topic 
partition and writes these updates to its state store and changelog topic. The changelog topic is 
replicated into the state stores of servers `B` and `C`. 

Now assume  LB receives a pull query (1) and sends the query to `B` (2). `B` determines that `A` 
is the active server and forwards the request to `A` (3). `A` executes the query successfully and 
returns the response.

Failure scenario: Assume that `A` goes down. `B` receives the request, tries to forward it to `A` 
and fails. What to do now? The current implementation tries to forward the request to `A` 
(in a busy loop) for a configurable timeout `KSQL_QUERY_PULL_ROUTING_TIMEOUT_MS` . If it has not 
succeeded in this timeout, the request fails. The next request `B` receives, it will again try to 
forward it to `A`, since `A` is still the active, and it will fail again. This will happen until 
rebalancing completes and a new active is elected for the partition. 

There are two steps in the rebalancing procedure that are expensive: 
  1) One of `B` or `C` is picked as the new active, this takes ~10 seconds based on default configs. 
  2) Once a new active is chosen, depending on how far behind (lag) it is with respect to the changelog, 
  it may take few seconds or even minutes before the standby has fully caught up to the last committed offset. 

In total, it takes >>10 seconds before a query can start succeeding again. What can we do until a
new active is ready to serve requests? 
[KIP-535](https://cwiki.apache.org/confluence/display/KAFKA/KIP-535%3A+Allow+state+stores+to+serve+stale+reads+during+rebalance) 
has laid the groundwork to allow us to serve requests from standbys, even if they are still in 
rebalancing state or have not caught up to the last committed offset of the active. 

Every KSQL server (active or standby) now has the information of:

1. Local current offset position per partition: The current offset position of a partition of the changelog topic that has been successfully written into the state store
2. Local end offset position per partition: The last offset written to a partition of the changelog topic
3. Routing Metadata for a key: The partition, active host and standy hosts per key

This information allows each server (with some communication, discussed below) to compute the global 
lag information of every topic partition whether it is the standby or active. This enables us to 
implement a policy where `B` having established that `A` is down (after trying to send a request 
to `A` and seeing it failed), to decide whether it (`B`) will serve it or `C`. This effectively 
means that there will be little to no down time in serving requests at the cost of consistency as
`B` or `C` may serve stale (not caught up) data. Hence, we achieve high availability at the cost of 
consistency. Eventual consistency for the win!


## What is in scope
Ensure availability for pull queries when KSQL servers fail (with `ksql.streams.num.standby.replicas=N`, 
we can tolerate upto N such failures) 

## What is not in scope

- Address unavailability caused by cold starts of KSQL server i.e when a new server is added to a 
KSQL cluster, it must first rebuild its state off the changelog topics and that process still could 
take a long time. 
- Try to improve consistency guarantees provided by KSQL i.e reduce the amount of time it takes to 
rebalance or standby replication to catch up. 

## Value/Return

The cluster of KSQl server will be able to serve requests even when the active is down. This mitigates 
a large current gap in deploying KSQL pull queries for mission critical use-cases, by significantly 
reducing failure rate of pull queries during server failures.

## Public APIS
We will support two ways to set the maximum acceptable lag (`acceptable.offset.lag`): Via each pull query and via a configuration parameter to the `WITH` clause of CTAS statements. These configs will be available to be set also in the json request.

## Design

The problem at hand is how to determine if a server is down and where to route a request when it is 
down. Every KSQL server must have available the information of what other KSQL servers exist in the 
cluster, their status and their lag per topic partition. There are three components to this: 
Healthchecking, Lag reporting and Lag-aware routing.  

### Failure detection/Healthcheck

Failure detection/ healthcheck is implemented via a periodic hearbeat. KSQL servers either broadcast 
their heartbeat (N^2 interconnections with N KSQL servers) or we implement a gossip protocol. In 
the initial implementation, we will use the REST API to send heartbeats leveraging the N^2 mesh that 
already exists between KSQL servers. Hence, we will implement a new REST endpoint, `/heartbeat` that 
will register the heartbeats a server receives from other servers. 

Cluster membership is determined using the information provided from the 
[Kafka Streams](https://kafka.apache.org/20/javadoc/org/apache/kafka/streams/KafkaStreams.html)
instance and specifically [`StreamsMetadata`](https://kafka.apache.org/20/javadoc/org/apache/kafka/streams/state/StreamsMetadata.html) 
from which we can obtain the information of the host and port of the instance. A server periodically
polls for all currently running KS instances (local and remote) and updates the list of remote servers
it has seen. If no KS streams are currently running, cluster membership is not performed. Moreover,
this policy depends on the current design where a new KSQL server replays every command in the 
command topic and hence runs every query.

We want to guarantee 99% uptime. This means that in a 5 minute window, it must take no longer than 
2 seconds to determine the active server is down and to forward the request to one of the standbys. 
The heartbeats must be light-weight so that we can send multiple heartbeats per second which will 
provide us with more datapoints required to implement a well-informed policy for determining when a 
server is up and when down.

Configuration parameters:
1) Heartbeat SEND interval (e.g send heartbeat every 100 ms)
2) Heartbeat WINDOW size (e.g valid heartbeat to consider when counting missed/received heartbeats 
and determine if server is down/up)
3) Heartbeat CHECK interval (e.g. process received heartbeats and determine server status every 200 ms)
4) MISSED_THRESHOLD: How many heartbeats in a row constitute a node as down (e.g. 3 missed heartbeats = server is down)
5) RECEIVED_THRESHOLD: How many heartbeats in a row constitute a node as up (e.g. 2 received heartbeats = server is up)

Description of how these configuration parameters are used:

Every server sends a heartbeat every SEND interval. Every server processes its received heartbeats via the 
`decide-status` process every CHECK interval. The `decide-status` process considers only heartbeats that are 
received from `windowStart = now - WINDOW` to `windowEnd = now`. Heartbeats that were received before `windowStart`
are expunged. The `decide-status` process counts the number of missed and received heartbeats in one window by 
checking whether there was a heartbeat received every SEND interval starting from `windowStart`. For example, 
from `windowStart=0` and `SEND=100` it will check if there was a heartbeat received at timestamp 0, 100, 200, 300 
etc. until `windowEnd`. If there is a timestamp for which no heartbeat was received, the process increases the 
missed count. If there are more than MISSED_THRESHOLD heartbeats missed in a row, then a server is marked as down.

We will provide sane defaults out-of-box, to achieve a 99% uptime.

Pseudocode for REST endpoint for `/heartbeat`:
```java
Map<KsqlServer, List<Long>> receivedHeartbeats;

@POST
public Response receiveHeartbeat(Request heartbeat) 
  return Response.ok(processRequest()).build();
}

private HeartbeatResponse processRequest() {
  KsqlServer server = request.getServer();
  receivedHeartbeats.get(server).add(request.getTimestamp());
}
```

Additionaly, we will implement a REST endpoint `/clusterStatus` that provides the current status of the cluster
i.e. which servers are up and which are down (from the viewpoint of the server that receives the
request).

Pseudocode for REST endpoint for `/clusterStatus`:
```java
Map<KsqlServer, Boolean> hostStatus;

@GET
public Response checkClusterStatus(Request heartbeat) 
  return Response.ok(processRequest()).build();
}

private ClusterStatusResponse processRequest() {
  return hostStatus;
}
```
### Lag reporting

Every server neeeds to periodically broadcast their local current offset and end offset positions. We will implement a new 
REST endpoint `/reportlag`. The local offsets information at a server is obtained via 
`Map<String, Map<Integer, LagInfo>> localPositionsMap = kafkaStreams.allLocalStorePartitionLags();`.

Pseudocode for REST endpoint for `/reportlag`:
```java
Map<KsqlServer, Map<String, Map<Integer, LagInfo>>> globalPositionsMap;

@POST
public Response receiveLagInfo(Request lagMap) 
  return Response.ok(processRequest()).build();
}

private LagReportResponse processRequest() {
  KsqlServer server = request.getServer();
  Map<String, Map<Integer, LagInfo>>  localPositionsMap = request.getPositions();
  globalPositionsMap.put(server, localPositionsMap);
}
``` 

### Lag-aware routing

Given the above information (alive + offsets), how does a KSQL server decide to which standby to route 
the request? Every server knows the offsets (current and end) of all partitions hosted by all servers in the cluster. 
Given this information, a server can compute the lag of itself and others by determining the maximum end offset
per partition as reported by all server and subtract from it their current offset. 
This allows us to implement lag-aware routing where server `B` can a) determine that server `A` is 
down and b) decide whether it will serve the request itself or forward it to `C` depending on who 
has the smallest lag for the given key in the pull query. 

Pseudocode for routing:
```java
// Map populated periodically through heartbeat information
Map<KsqlNode, Boolean> aliveNodes;

// Map populated periodically through lag reporting
// KsqlServer to store name to partition to lag information
Map<KsqlServer, Map<String, Map<Integer, LagInfo>>> globalPositionsMap;
 
// Fetch the metadata related to the key
KeyQueryMetadata queryEntity = queryMetadataForKey(store, key, serializer);
 
// Acceptable lag for the query
final long acceptableOffsetLag = 10000;

// Max end offset position
Map<String, Map<Integer, Long>> maxEndOffsetPerStorePerPartition;
for (KsqlServer server:  globalPositionsMap) {
    for (String store: globalPositionsMap.get(server)) {
        for (Integer partition: globalPositionsMap.get(server).get(store)) {
            long offset = globalPositionsMap.get(server).get(store).get(partition);
            maxEndOffsetPerStorePerPartition.computeIfAbsent(store, Map::new);
            maxEndOffsetPerStorePerPartition.get(store).computeIfAbsent(partition, Map::new);
            maxEndOffsetPerStorePerPartition.get(store).putIfAbsent(partition, -1);
            long currentMax = maxEndOffsetPerStore.get(store).get(partition);
            maxEndOffsetPerStorePerPartition.get(store).put(partition, Math.max(offset, currentMax);       
        }
    }
}

// Ordered list of servers, in order of most caught-up to least
List<KsqlNode> nodesToQuery;
KsqlNode active = queryEntity.getActiveHost();
if (aliveNodes.get(active)) {
  nodesToQuery.add(active)  
}

// add standbys
nodesToQuery.addAll(queryEntity.getStandbyHosts().stream()
    // filter out all the standbys that are down
    .filter(standbyHost -> aliveNodes.get(standByHost) != null)
    // get the lag at each standby host for the key's store partition
    .map(standbyHost -> new Pair(standbyHostInfo,
        maxEndOffsetPerStorePerPartition.get(storeName).get(queryEntity.partition()) - 
        globalPositionsMap.get(standbyHost).get(storeName).get(queryEntity.partition()).currentOffsetPosition))
    // Sort by offset lag, i.e smallest lag first
    .sorted(Comaparator.comparing(Pair::getRight())
    .filter(standbyHostLagPair -> standbyHostLagPair.getRight() < acceptableOffsetLag)
    .map(standbyHostLagPair -> standbyHostLagPair.getLeft())
    .collect(Collectors.toList()));
 
// Query available nodes, starting from active if up
List<Row> result = null;
for (KsqlNode server : nodesToQuery) {
  try {
    result = query(store, key, server);
  } catch (Exception e) {
    System.err.println("Querying server %s failed", server);
  }
  if (result != null) {
    break;
  }
}

if (result == null) {
  throw new Exception("Unable to serve request. All nodes are down or too far behind");
}

return result;
```

We will also introduce a per-query configuration parameter `acceptable.offset.lag`, that will provide 
applications the ability to control how much stale data they are willing to tolerate on a per query
basis. If a standby lags behind by more than the tolerable limit, pull queries will fail. 
This parameter can be configured either as part of the `WITH` clause of CTAS queries or be given as
arguments to the request's JSON payload. This is a very 
useful knob to handle the scenario of cold start explained above. In such a case, a newly added KSQL 
server could be lagging by a lot as it rebuilds the entire state and thus the usefulness of the data 
returned by pull queries may diminish significantly.

### Tradeoffs
1. We decouple the failure detection mechanism from the lag reporting to make the heartbeats 
light-weight and achieve smaller heartbeat interval. This way, heartbeats can be sent at a higher 
interval than lag information (which is much larger in size). As our goal is to achieve high-availability, 
receiving less frequent lag updates is ok as this affects consistency and not availability.
2. We decouple routing decision from healthchecking. The decision of where to route a query is local 
(i.e. does not require remote calls) as the information about the status of other servers is already 
there. This provides flexibility in changing the lag reporting mechanism down the line more easily.
3. We choose to keep the initial design simple (no request based failure detection, gossip protocols) 
and closer to choices made in Kafka/Kafka Streams (no Zookeeper based failure detection), for ease 
of deployment and troubleshooting. 

### Rejected/Alternate approaches

#### Retrieve lag information on-demand
We employ a pull model where servers don't explicitly send their lag information. Rather, communication 
happens only when needed, i.e. when a server tries to forward a request to another server. Once 
server `B` has determined that server `A` is down, it needs to determine what other server should 
evaluate the query. At this point,`B` doesn't have any knowledge of the lag information of the other 
standbys. So, in addition to evaluating the query locally, `B` also sends the request to `C`. `B` 
then has both its own result and `C`’s result of query evaluation and decides which one is the freshest 
to include in the response. `B` can make this decision because the lag information is piggybacked 
with the query evaluation result. The advantages of this approach is that it results in less 
communication overhead: Lag information is exchanged only when the active is down. Moreover, it is 
piggy-backed on the request. On the other side, all standbys need to evaluate the same query. Moreover, 
the communication between `B` and `C` involves large messages as they contain the query result 
(can be many rows). Finally, latency for a request increases as `B` needs to wait to receive a 
response from `C` with query result and lag information. Then only can `B` send a response back to 
the client. 

#### More efficient lag propagation
Instead of broadcasting lag information, we could also build a gossip protocol to disseminate this 
information, with more round trips but lower network bandwidth consumption. While we concede that 
this is an effective and proven technique, debugging such protocols is hard in practice. So, we 
decided to keep things simple, learn and iterate. Similarly, we could also encode lag information 
in the regular pull query responses themselves, providing very upto-date lag estimates. However, 
sending lag information for all local stores in every response will be prohibitively expensive and 
we would need a more intelligent, selective propagation that only piggybacks a few stores's lag in 
each response. We may pursue both of these approaches in the future, based on initial experience.

#### Failure detection based on actual request failures
In this proposal, we have argued for a separate health checking mechanism (i.e separate control plane), 
while we could have used the pull query requests between servers themselves to gauge whether another 
server is up or down. But, any scheme like that would require a fallback mechanism that periodically 
probes other servers anyway to keep the availability information upto date. While we recognize that 
such an approach could provide much quicker failure detection (and potentially higher number of 
samples to base failure detection on) and less communication overhead, it also requires significant 
tuning to handle transient application pauses or other corner cases.

We intend to use the simple heartbeat mechanism proposed here as a baseline implementation, that can 
be extended to more advanced schemes like these down the line.

## Test plan

We will do unit tests and integration tests with failure scenarios where we cover the cases:
1. Request is forwarded to a standby.
2. Request is forward to the most caught-up standby.
3. Request fails if lag of standbys is more than acceptable lag configuration.

We will look into muckrake or cc-system-tests.
## Documentation Updates

Need to add documentation about the configuration parameters regarding the failure detection policy, 
acceptable lag, new endpoints. 

# Compatibility Implications

N/A

## Performance Implications

Improve performance of pull queries in case of failure.

## Security Implications

N/A
