Create a config file with all the node details
- [ ] config.go
- [ ] 

Reads
- Load balanced reads: being able to service a read on any replica
- Completing the read locally: no communication with other nodes required


Writes
- Any replica must be able to initiate a write and drive to completion
- Inter-key concurrent: Independent writes on different keys should be able to proceed in parallel to enable intra and multi-threaded parallel request execution.

Failure model
- crash-stop 
- non-byzantine behaviour
- network failures: a) message re-ordering, duplication, loss b) link failures, n/w partitioning

