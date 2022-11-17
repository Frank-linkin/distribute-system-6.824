实现建议：
最好画出详细的follower,Candidate,Primary状态转换的状态图。
考虑好就好了，毕竟输入只有appendEntry和requestVote

Test2B 20221112
Leader behavior
receive request
1. write the command to its log
2. for each follower
        while AppendEntries 失败
            重试AppendEntries
   commited的条件是什么？
   得到了majority的vote and 
   adding constriaints, if commited
        applay entries to its satemachine

可能需要像课堂一下画一下“一个Leader被选出来之后发生的事情”，这样可以更快的实现BugFree的代码。


[Question]
heartBeat如果检测到follower的stale data，要不要给它信息让它catch up
1.如果要，那么可能就会有重复的data被发送到follower。
并且，不符合5.4.2
[answer]不对，是符合5.4.2的
2.如果不要，那么只有接到request的时候，stale follower才会catch up，显然这是不对的。

[Question]
关于as update-to-date as receiver这个条件的判断,paper中有5.4.1有解释。但是感觉不对。
如果
S1 1 2 
S2 1 3 3 3
S3 1 3 3 3
如果s1是Leader，失去了和各个机器的连接，那么它的term会不停增加。
[answer]
不对，作为leader，它的term永远不会增加，那它再次连接上一定不会被选为Leader
[followerUp]
若判断条件是candidate.Term>=my.Term&&candidate的log长度>=my.Log长度，这个判断条件可以使用吗？
先暂时用这个


三种apendEntries:HearBeat,AppendNewEntries,AppendBackupEntries
1.hearBeat
Input：无
effect:重置ElectionTimer
如果返回success==false,根据传回来的信息重置nextIndex[server],并向其重新发送这个数据

2.AppendNewEntries
Input:NewEntry

affect:
rf.nextIndex[server]++
如果majorty都成功AppendNewEntries,rf.commitIndex+1,回复Client

3.AppendBackupEntries
rf.nextIndex[server]+=args.logEntries.length
如果返回success==false,根据传回来的信息重置nextIndex[server],并向其重新发送这个数据
if rf.nextIndex[server]

关于backup
如果选择每次发送一个请求，或者每次HeartBeat的时候都会向follower发送一份它现在缺少的Term的数据
这里有个trade off. 这样会导致catch up可能不太及时，比如一个follower它有stale的数据，那么
它一定不会commit当前request，会导致投票成功的概率较少。但是因为并没有向客户端返回，并不算commit。
还有一种方法是,Leader发现某个Server有stale数据后，立马开启一个线程立马地去补这个数据。
相应的补这个数据可能会与hearBeat补的是相同的数据。
感觉第一种方法好一点。


appendEntries will retry until it success.也就是说appendEntries的结果，from Leader's perspective：
1.complete a term in a stale follower 或 replicate a new command to a follower

### 关于AppendNewEntry,HeartBeat和Catchup
1.AppendNewEntry就会将Command发送出去，如果是stale follower，就会拒绝这次appendEntry.
2.HeartBeat的信息还是nil，但是会根据结果更新next[server]
因为heartBeat只有40ms，所以还是想直接在HeartBeat中补齐stale的数据
方案二
有stale follower一个staleCh,staleCh有数据就往follower中发送。
选第二种。

只有heartBeat的时候会向前修改nextIdx.CatchUpWorker也会向前修改nextIdx。

catupWorker
获取chan中所有nextIndex，取出最小的，发送出去
考虑heartbeat
从nextIndex[server]发出一个nil。获取的结果有：
1.成功，匹配，且nextIndex[server]>=commitIndex
server是up-to-date的
2.成功，匹配，且nextIndex[server]< commitIndex
证明是stale follower，向catUpWorker发送nextIndex
3.失败，不匹配，
向catchUpWorker发送更新后的nextIndex
考虑CatchupWorker
1.成功，匹配，等待下一个Signal
2.失败，不匹配，更新nextIndex后给自己更新signal
这样这个逻辑实现了：每次heartbeat都会让stale follower更新一个term
catupWorker的chan是用带缓冲的还是不带缓冲的。
1.带缓冲，每次heartbeat直接往ch塞数据就可以了
2.不带缓冲，每次heartBeat尝试往ch里面塞数据
选第二种

AppendEntries,success=false的响应步骤改进改进



tryToCommit
如果Leader将一个Command写入disk，但是还有commit，现在又来个一个command。这时候肯定不是用同一个logIndex。会出现一个问题，follower backup第一个commnd可能不是在tryToCommit产生的appendEnties产生的，而是被Worker修补到的，那么就没办法进行RequestVote这种计票。

rf里面用edit记录修改nextIndex的Follower的数量，超过半数的时候，更commmitIndex，并将新的commmitIndex进行广播。这样可以让等待的request进行返回。
1.request可以快速返回，使用counter
2.如果一定时间内没有返回，就监听commmitChan,
感觉快速返回可以取消，

TODO:
1.
对sentAppendEntrie的操作应该是统一的，看看能不能统一起来
2.梳理一下逻辑，准备测试
现在有一个commitUpdate每10ms周期检查一次commit的update

当RequestVote的时候，LastLogIndex是commitIndex，还是写到磁盘上的最高的logEntry的Index?
应该是写到磁盘上最后的logEntry的Index


TODO:确定一个diskLogIndex的初值正确，应该为0
TODO: 实验说明上提到的applyCh在哪