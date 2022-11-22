# go-parallel-graphsync

### Project Description

Currently, most projects in Filecoin Retrieval Market use GraphSync to synchronize data.

GraphSync works well when synchronizing small-volume files. Users can synchronize these multiple files from different nodes by downloading them in parallel. This way could spread the bandwidth pressure of one single node and shorten the synchronization time.

However, this one-to-one synchronizing method may not work well for large files. If data synchronization time becomes longer,  from minutes to tens of minutes or even hours, a single node's bandwidth load will increase. Besides, other nodes with the same data do not provide retrieval services, so resources in the Filecoin network can not be fully utilized.

ParallelGraphSync aims to provide a solution for synchronizing large files in Filecoin Retrieval Market. It can dynamically adjust the synchronization strategy of data blocks to achieve parallel download by connecting multiple nodes with retrieved data and measuring the synchronization speed of the nodes.

For example, we assume Node A needs to synchronize a 1GiB file and Node B, C, and D have duplicate file copies. Network delays between Node A and other nodes are:

- A to B: 10ms
- A to C: 50ms
- A to D: 100ms

The transmission speed from Node B, C, and D to A are all 1MB/s.

![comparison diagram](./doc/compare.png)

The results could be:

- By using GraphSync, it takes about 17mins to synchronize this file from Node B(optimal) to Node A.
- By using ParallelGraphSync, if we adjust the synchronization strategy according to their network delay and transmission speed, it takes about 5mins 42s to complete the same work.

The actual situation will be more complicated and time-consuming but will not affect parallel synchronization's advantages.

For one-to-one data synchronization, the increase in data copies only increases the throughput of a CDN network. The data retrieval speed can not improve significantly and is limited to a single node's transmission speed.
With ParallelGraphSync, the number of data copies can increase the entire network's throughput and significantly improve the retrieval speed, especially for large files.



### Value

- Shift traffic from a single node to multiple nodes

- Spread the bandwidth pressure of one single node
- Increase the speed of content delivery
- Improve the utilization of network resources


### Benchmark
```shell
go test -v -test.run '^Bench.*$' -test.bench 'BenchmarkGraphSync' ./example/ -benchtime=20x --benchmem

requester peerId=12D3KooWQ4zg9BTGhaF3NfaR9S4vCQJ6WAZ6VvCyNJMQkJaDMrHe
goos: darwin
goarch: amd64
pkg: github.com/filedrive-team/go-parallel-graphsync/example
cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
BenchmarkGraphSync
BenchmarkGraphSync/Parallel-Grapysync_request_to_3_service
BenchmarkGraphSync/Parallel-Grapysync_request_to_3_service-12                 20         291028110 ns/op        934336510 B/op    169788 allocs/op
BenchmarkGraphSync/Grapysync_request_to_1_service
BenchmarkGraphSync/Grapysync_request_to_1_service-12                          20         579072270 ns/op        932883964 B/op    167104 allocs/op
PASS
ok      github.com/filedrive-team/go-parallel-graphsync/example 19.798s
```