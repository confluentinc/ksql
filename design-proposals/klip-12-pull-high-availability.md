# KLIP 12 - Implement High-Availability for Pull queries

**Author**: Vicky Papavasileiou, Vinoth Chandar | 
**Release Target**: 5.5 | 
**Status**: _In Discussion_
**Discussion**: _link to the design discussion PR_

**tl;dr:** Enables high-availability for Ksql servers in case of failures. Sacrifices consistency, by serving stale data from laggy standbys in the name of availability. Improves upon the current architecture where in case of failure of the active ksql server, there is a window of several seconds (10+) to minutes of unavailability due to the rebalancing procedure.

## Motivation and background

Assume we have a load balancer (LB) and a cluster of three KSQL servers (A, B, C) where `A` is the active server and `B`, `C` are the standbys. `A` receives updates from the source topic partition and writes these updates to its state store and changelog topic. The changelog topic is replicated into the state store of servers `B` and `C`. Note, every Ksql server is the active for some partitions and standby for others. Without loss of generality, we will focus on one partition for now.

Now assume  LB receives a pull query (1) and sends the query to `B` (2). `B` determines that `A` is the active server and forwards the request to `A` (3). A executes the query successfully and returns the response.

Failure scenario: Assume that `A` goes down. `B` receives the request, tries to forward it to `A` and fails. What to do now? The current implementation tries to forward the request to `A` (in a busy loop) for a configurable timeout `KSQL_QUERY_PULL_ROUTING_TIMEOUT_MS` . If it has not succeeded in this timeout, the request fails. The next request `B` receives, it will again try to forward it to `A`, since `A` is still the active, and it will fail again. This will happen until rebalancing completes. 

There are two steps in the rebalancing procedure that are expensive: 1) One of `B` or `C` is picked as the new active, this takes ~10 seconds based on default configs. 2) Once a new active is chosen, depending on how far behind (lag) it is with respect to the changelog, it may take up  few seconds or even minutes before the standby has fully caught up to the last committed offset. In total, it takes >>10 seconds before a query can start succeeding again.
 
What can we do until a new active is ready to serve requests? 
[KIP-535](https://cwiki.apache.org/confluence/display/KAFKA/KIP-535%3A+Allow+state+stores+to+serve+stale+reads+during+rebalance) has laid the groundwork to allow us to serve requests from standbys, even if they have not caught up to the last committed offset of the active. 

Every ksql server now has the information of:

1. Local offset lag: The lag of all partitions the server hosts (whether active or standby)
2. Routing Metadata for a key: The partition, active host and standy hosts per key

This information allows each server (with some communication, discussed below) to know the global lag information of every topic partition whether it is the standby or active. This enables us to implement a policy where `B` having established that `A` is down (after trying to send a request to `A` and seeing it failed), to decide whether it (`B`) will serve it or `C`. This effectively means that there will be little to no down time in serving requests at the cost of consistency as `B` or `C` may serve stale (not caught up) data. Hence, we achieve high availability at the cost of consistency. Eventual consistency for the win!


## What is in scope

Ensure availability for pull queries when the active KSQL server fails.

## What is not in scope

_What we explicitly do not want to cover in this proposal, and why._

> We will not ______ because ______.  Example: "We will not tackle Protobuf support in this proposal 
> because we must use schema registry, and the registry does not support Protobuf yet."

## Value/Return

The cluster of KSQl server will be able to serve requests even when the active is down. Hence, pull queries will not fail during the time window required by rebalancing. 

## Public APIS
No change to public APIs

## Design

The problem at hand is how to determine if a server is down and where to route a request when the active is down. Every KSQL server must have available the information of what other KSQL servers exist in the cluster, their status and their lag per topic partition. There are three components to this: Healthchecking, Lag reporting and Lag-aware routing.  

***Failure detection/Healthcheck***

Failure detection/ healthcheck is implemented via a periodic hearbeat. KSQL servers either broadcast their heartbeat (N^2 interconnections with N KSQL servers) or we implement a gossip protocol. In the initial implementation, we will use the REST API to send heartbeats leveraging the N^2 mesh that already exists between KSQL servers. Hence, we will implement a new REST endpoint, `/heartbeat` that will register the requests(hearbeats) a server receives from other servers. 

We want to guarantee 99% uptime. This means that in a 5 minute window, it must take no longer than 2 seconds to determine the active server is down and to forward the request to one of the standbys. The heartbeats must be light-weight so that we can send multiple heartbeats per second which will provide us with more datapoints required to implement a well-informed policy for determining when a server is up and when down.

Configuration parameters:
1) Heartbeat INTERVAL (e.g send heartbeat every 200 ms)
2) Heartbeat WINDOW (e.g every 2 seconds, count the missed/received heartbeats and determine if server is down/up)
3) MISSED_THRESHOLD: How many heartbeats in a row constitute a node as down (e.g. 3 missed heartbeats = server is down)
4) RECEIVED_THRESHOLD: How many heartbeats in a row constitute a node as up (e.g. 5 received heartbeats = server is up)

Pseudocode for REST endpoint for `/heartbeat`:
```java
Map<KsqlServer, List<Long>> receivedHeartbeats;

@POST
public Response receiveHeartbeat(Request heartbeat) 
  return Response.ok(processRequest()).build();
}

private HeartbeatResponse processRequest() {
  KsqlServer server = request.getServer();
  receivedHeartbeats.get(server).add(System.currentTimeMillis());
}
```

Pseudocode for decision-making policy:
```java
Map<KsqlNode, Boolean> serverStatus;
long prev_time;

while (true) {
  if (current_time - prev_time > WINDOW) {
    for (KsqlServer server: receivedHeartbeats) {
      if(receivedHeartbeats.get(server).size() < RECEIVED_THRESHOLD) {
        serverStatus.put(server, false); //server is down
      } else {
        serverStatus.put(server, countHeartbeats(server, receivedHeartbeats.get(server));
        receivedHeartbeats.get(server).clear();
      }  
    }
  }
}
```

***Lag reporting***

Every server neeeds to periodically broadcast their local lag information. We will implement a new REST endpoint `/reportLag`. The lag information at a server is obtained via `Map<String, Map<Integer, Long>> localLagMap = kafkaStreams.allLocalOffsetLags();`.

Pseudocode for REST endpoint for `/reportLag`:
```java
Map<KsqlServer, Map<String, Map<Integer, Long>> globalLagMap;

@POST
public Response receiveLagInfo(Request lagMap) 
  return Response.ok(processRequest()).build();
}

private LagReportResponse processRequest() {
  KsqlServer server = request.getServer();
  Map<String, Map<Integer, Long>> localLagMap = request.getLag();
  globalLagMap.put(server, localLagMap);
}
``` 

***Lag-aware routing***

Given the above information (alive + lag), how does a KSQL server decide to which standby to route the request? Every server knows the lag of all partitions hosted by all servers in the cluster. This allows us to implement lag-aware routing where server `B` can a) determine that server `A` is down and b) decide whether it will serve the request itself or forward it to `C` depending on who has the smallest lag for the given key in the pull query. 

Pseudocode for routing:
```java
// Map populated periodically through heartbeat information
Map<KsqlNode, Map<String, Map<Integer, Long>> allNodesLagMap;
 
// Fetch the metadata related to the key
KeyQueryMetadata queryMetadata = queryMetadataForKey(store, key, serializer);
 
// Acceptable lag for the query
final long acceptableOffsetLag = 10000;
 
if (serverStatus.get(queryMetadata.getActiveHost())) {
  // always route to active if alive
  query(store, key, queryMetadata.getActiveHost());
} else {
  // filter out all the standbys with unacceptable lag than acceptable lag & obtain a list of standbys that are in-sync
  List<HostInfo> inSyncStandbys = queryMetadata.getStandbyHosts().stream()
      // get the lag at each standby host for the key's store partition
      .map(standbyHostInfo -> new Pair(standbyHostInfo, allNodesLagMap.get(standbyHostInfo).get(storeName).get(queryMetadata.partition())))
      // Sort by offset lag, i.e smallest lag first
      .sorted(Comaparator.comparing(Pair::getRight())
      .filter(standbyHostLagPair -> standbyHostLagPair.getRight() < acceptableOffsetLag)
      .map(standbyHostLagPair -> standbyHostLagPair.getLeft())
      .collect(Collectors.toList());
 
  // Query standbys most in-sync to least in-sync
  for (HostInfo standbyHost : inSyncStandbys) {
    try {
      query(store, key, standbyHost);
    } catch (Exception e) {
      System.err.println("Querying standby failed");
    }
  }
```

***Discussion***

Advantages:

1. We decouple the failure detection mechanism from the lag reporting to make the heartbeats light-weight and achieve smaller heartbeat interval. This way, hearbeats can be sent at a higher interval than lag information (which is much larger in size). As our goal is to achieve high-availability, receiving less frequent lag updates is ok as this affects consistency and not availability.
2. We decouple routing decision from healthchecking. The decision of where to route a query is local (i.e. does not require remote calls) as the information about the status of other servers is already there. 

Drawbacks:

1. Lag information is trasmitted even when the active is up resulting in a lot of network communication. To address this, we can send only the delta of lag information (only what has changed from the previous time).

***Rejected approach***

We employ a pull model where servers don't explicitly send a hearbeat or their lag information. Rather, communication happens only when needed, i.e. when a server tries to forward a request to another server. When server `B` issues the first request to `A`, it needs to determine whether `A` is down. 

Configuration parameters:
1) Request TIMEOUT (e.g. how long to wait before failing a request)
2) Request WINDOW (e.g. how many requests to attempt before deciding a server is down)
3) MISSED_THRESHOLD: How many timed-out requests in a row constitute a server as down.
4) Request REMEMBER_STATUS: Once a server is marked down, when will we try again to forward requests to it (e.g. remember server `A` is down for 5 seconds. After 5 seconds, try again).

Once `B` has determined that server `A` is down, it needs to determine what other server should evaluate the query. At this point, `B` doesn't have any knowledge of the lag information of the other standbys. So, in addition to evaluating the query locally, `B` also sends the request to `C`. `B` then has both its own result and `C`’s result of query evaluation and decides which one is the freshest to include in the response. `B` can make this decision because the lag information is piggybacked with the query evaluation result.

**Discussion**

Advantages:

1. Less communication overhead: Lag information is exchanged only when the active is down. Moreover, it is piggy-backed on the request. 

Drawbacks:

1. All standbys evaluate the query.
2. The communication between `B` and `C` involves large messages as they contain the query result (can be many rows).
3. Latency for a request increases dramatically as routing decision is not local and blocks on healthchecking. `B` needs to wait until `A`'s response times out, then wait to receive a response from `C` with query result and lag information. Then only can `B` send a response back to the client. 

## Test plan

_What tests do you plan to write?  What are the failure scenarios that we do / do not cover? It goes 
without saying that most classes should have unit tests. This section is more focussed on the 
integration and system tests that you need to write to test the changes you are making._

## Documentation Updates

_What changes need to be made to the documentation? For example_

* Do we need to change the KSQL quickstart(s) and/or the KSQL demo to showcase the new functionality? What are specific changes that should be made?
* Do we need to update the syntax reference?
* Do we need to add/update/remove examples?
* Do we need to update the FAQs?
* Do we need to add documentation so that users know how to configure KSQL to use the new feature? 
* Etc.

_This section should try to be as close as possible to the eventual documentation updates that 
need to me made, since that will force us into thinking how the feature is actually going to be 
used, and how users can be on-boarded onto the new feature. The upside is that the documentation 
will be ready to go before any work even begins, so only the fun part is left._

# Compatibility Implications

_Will the proposed changes break existing queries or work flows?_

_Are we deprecating existing APIs with these changes? If so, when do we plan to remove the underlying code?_

_If we are removing old functionality, what is the migration plan?_

## Performance Implications

_Will the proposed changes affect performance, (either positively or negatively)._

## Security Implications

_Are any external communications made? What new threats may be introduced?_ ___Are there authentication,
authorization and audit mechanisms established?_
