# Scaling

- handled transparently
- just use the same service id
- caveats: have enough partitions to subsume number of nodes
- guidelines: when to scale, growing consumer lag
- utilization: if its going up add more nodes. If not its something else. Look for bottlenecks