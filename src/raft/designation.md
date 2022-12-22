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

20221119
[knowledge]尽量写易读的代码，这样调试起来会非常容易。

from Persist22C, we can indicate commitIndex should also be persist. 这一点在论文里根本没有提到。



TODO:觉得这种设计思路得记下来，比如commit为什么不能用投票，已经记不清楚了。

[knowledge]当一个模块设计的时候漏考虑了某件事，不能简单到缝缝补补，越是缝缝补补越容易
出难以预料的BUg。缝补过多的时候，就需要将这个模块的思路整体都思考一遍，然后用新思考的逻辑
重新编写代码。比如，我在设计CatchUpWorker的时候没有考虑到matchIdx，后来再2C的某个实验中
MatchIdx是必须要使用的。于是我简单将我可以想到的，设计MatchIndex的地方更改的了一下，导致总是莫名其妙出很多很多问题。
这时候，其实应该：
重新捋一遍AppendEntrie和CatchUpWorker的逻辑，明确他们在考虑是做什么的，调整代码。确定逻辑没问题，再进行测试。

AppendEntrie:
1.接到心跳->初上位的心跳 prevLogTerm != args.Term
          success = true
          ->稳定后的心跳 prevLogTerm == args.Term
          判断prevLogTerm是否匹配，->匹配 sucess =  true
                                   ->不匹配 succes = true，发送xterm
2.接到带数据的AppendEntry
  -     判断是否prevLogTerm是否匹配， ->匹配则使用它
  -                                  ->不匹配发送Xterm

Server接到AppendEntry的response:
1.response != success 
    计算nextIndex，发送backup信号
    发送backup信号
2.reponse == succeess
    if data != nil 
        更新matchIndex和nextIndex
    if data == nil
        do nothing


TODO:
已测试20次2C,晚上还会测试50次。

20221128
[knowledge]
我在写更新commit的逻辑的时候，考虑了前面applyLogEntryIntoDisk后，认为在不论何种情况下，执行完applyLogEntryIntoDisk之后，disklogIndex应该就更新了。
但是其实还有一种diskLogIndex没有更新的情况，导致出现了一个很难发现的问题，debug消耗了六七个小时的时间。
如果我在当时写的时候，就点回去applyLogEntryIntoDisk看一下逻辑，其实这个是可以避免。
我觉得在写函数的时候，一定要把它默认的情况写出来，比如adjustCommitIndex就需要当前diskLogIndex之后没有logEntry了。然后在最后
测试之后，git commit之前把这些问题都删除，放到其他的地方。

[knowledge]
一个Server变成candidate,requestVote请求返回之后，比较了args.term，根据结果让其变成follower，这是不对的。因为requestVote请求可能很久之后抵达，
而Server可能隔了好几个Term又变成了Leader，那么这时候Leader就会step，并且currentTerm可能会减少，造成这种问题。Log1720行左右
其实已经AppendEntry已经出现过这种问题，当时就应该联想到RequestVote可能也会出现这种情况，把requestVote的逻辑更改过来。这样就可以避免这1hour30min的时间浪费。

##commitApplier设计
type struct {
    applyCh
    commitIndex
    applyIndex
}

UpdateCommitIndex()//更新commitIndex{
    会向更新goroutine发出一个信号
}
