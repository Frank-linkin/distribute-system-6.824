
branch bug/invalid_backup_crowed_out_valid

每个heartbeat会发送当前rf.diskLogIndex作为prevLogIndex。本来这次参数在网络情况良好的情况下没有问题，但是一旦网络环境很糟的话，就会出问题。raft原文是很笼统的，没有涉及这方面。
   0 1 2 3 4 5 6 7 8 9 10 11   
S0 1 1 1 7 7 7 8 8 8 9 9  9 
S1 1 1 1 3 3 3 4 4 4 5 5  5
s2 1 1 1 7 7 7 8 8 8 9 9  9
如果s1需要catchup的话，它会
1.s1发送xIndex=5,然后s0会发送999
2.s1发送[4,4,4]，然后s0会发送888
3.s1发送[333],然后s0会发送777
但是如果以上任意一条request，delay的很长时间，但依然成功了。但是因为有hearbeat，heartbeat发送的prevLogIndex=9，所以，s1会用555回应心跳，会向catupWorker发送999，因为经历了很多次heartbeat，那么CatchUpWorker的缓冲区就被填满，所以新的Xindex=6,xIndex=3的信息都发送不到catchupWorker里面去了。
于是CatchupWorker依然会发送999，然后后面的序号的request又有一个delay了很长时间，然后就陷入死循环了

但是把heartbeat的prevLogIndex改为rf.nextIndex[server]的话也有一个问题。设Leader为S4,如果Leader上位之后只接到一个request，然后这个request在其他4台机器上都commit了，而发送给s2的丢失了，那么s2会一直保持拒绝sync的状态，因为它接到args中，prevLogTerm!=currentTerm。

1.选择heartBeat diskLogIndex,那么就增大buffer容量
2.选择heartBeat nextIde[server]
这里，我选择，heartBeat diskLogIndex，增大buffer容量