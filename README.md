HbaseLoadBalancerGit
====================

Idea: If a number of read requests to a region(or a set of regions) is more than a fixed ratio and fixed number, then it is said to be a hotspot.
Extended DefaultLoadBalancer of Hbase to HotSpotLoadBalancer, as hook is provided in configuration that can switch load balancer, the balancer needs to return RegionPlan which indicates the movement of regions that needs to be carried out.
HotSpotLoadBalancer is further extended as an example in to two load balancers, which use different parameters to calculate load.
More to come, readhotspot formula.
