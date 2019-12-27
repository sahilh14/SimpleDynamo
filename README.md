# SimpleDynamo

A simplified version of Amazon Dynamo using Android based system containing 5 Android Virtual Devices (AVDs).

Comprises of three main parts :-
1. Partitioning: Network was partitioned into 5 nodes corresponding to 5 AVDs.
2. Replication: Quorum replication was implemented to store replicas of data in the networking nodes.
3. Failure handling: Replicas were used for failure handling and recovery such that normal working is not disturbed.
