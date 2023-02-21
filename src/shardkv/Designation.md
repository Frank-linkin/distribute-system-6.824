如果GroupB有Shard1发现了新的Config，shard1属于GroupA。然后需要将Shard1传输到GroupA。

- 正常情况
keys2Shard()->put()


- 特殊情况1
keys2Shard()->shard已经转移->这时候是重新去要config，还是直接让该Server指路呢？

- 配置变更的情况
如果检测到配置变更，这个Server不再是这个Shard的服务提供者，那么就停止这个(put,get,append)操作，向Client返回一个Err_NOT_SHARD_OWNER。这样Client会不断重试这个请求。另一方面，另一个GroupB会开始接受这个Shard，当这个Shard传输完成之后，Group会开始提供这个Shard的operation service。
RPC-transferShard:
这里还是选用新的GroupB拉的方式，GroupB如果不成功，会不停地重试。
RPC-TransferShardFinished:
GroupB向GroupA告知已经传输完成，可以清理掉Shard的相关数据。

考虑GroupB在Shard还未传输过来的时候，向Client发送Err_SHard_not_prepared。（废弃掉，不会减少Client的端的工作）

- 看样子会有Concurrent reconfiguration，也就是一下子配置更新好几个版本
一个Group的Leader应该周期性的询问配置更新，如果有更新，就返回下一个Config，记为Config2，装配好Config2后，继续更新，直到变成最新的config3。

有了Config3后带着ConfigVersionNo向Group询问Shard掌握情况，找到这个ShardOwner，然后向其请求此Shard的数据。

GroupB在收到ShardOwner查询请求之后，向其反馈其Shard的掌握情况。

- 关于shard的转移
map[shardNo]*ShardStruct
type ShardStruct struct {
    storage map[string]string
    valid bool//表示这个storage是否有效
    mu deadlock.Mutex
}

Shard的请求就直接把storage发送过去就好

一个数据结构CleanNotifier用来通知上一个ShardOwner删除Shard数据
CleanNotifier:1.当前所有Shard的通知状态，通知成功或通知失败


如果GroupB发现了新的Config，但是在Shard1传输成功之前依旧保持服务。
如果要配合这一改动的话，那Server需要知道当前所有的Shard的控制权情况。

Shard传输需要GroupA使用两个RPC，
1. RequestShard()
对于GroupB ShardSource:将Shard中的map[string]string发送出去。
并将valid置为false.
2. ConfirmShardReceived()
对于GroupB ShardSource:将Shard从数据库中删除
对于GroupA，就可以将valid置为true了，向外提供服务了。

GroupB探知到新的Config，什么都不做，依然提供Shard1的服务。直到这个Shard被请求。
GroupA探知到新的Config，立刻向这个Shard的拥有者请求这个Shard。
这就引出一个问题，GroupA怎么知道Shard1在哪个机器上呢。最简单的办法就是同时向所有机器发出请求（我称为全询操作），
感觉不需要维护一个Shard在哪的状态，因为增加了网络中的流量，不维护shardOwner的状态，只用一个全询就搞定了，但是如果要维护的话，就要每个机器每2-3秒发出一个全询来维护这个状态，所以还是不要维护得好。

GroupA启动的时候需要向ConfigService来请求最新的config，然后根据Config将自己own的Shard置为激活状态。

这里的crash情况共有下面几种
1. GroupA刚知道配置，或者还没有知道配置就挂了。GroupA重启的时候会拉一次配置，然后继续进行，不影响。
2. GroupA在RequestShard完成后挂了，那么启动后会激活Shard1的服务，但是Shard1老的版本就永远在GroupB内了。那么GroupB内放一个时钟，如果这个Shard.valid=false，且已经无效超过了1小时，那么就向外发出一个全询操作，全询是否有其他机器已经Own了Shard1，有就删除，没有就保留。
3. ConfirmShardReceived中挂了，GroupB已经删除，没有影响。
4. 在ConfirmShardReceived后，但是还没有把Shard1置为valid，依然不影响。

- 关于收到Config之后要干啥
使用Config，查看哪些Shard需要从别的机器请求，向别的Machine请求并安装到本Machine中。这里要对所有Machne进行一个全询。这里其实最好有一个线程池，这样可以不用启动太多线程来做全询。不过既然是实验，那就每个通信一个线程。

在此基础上，再减少通信量，就是记住上一个Config，注意这个上一个Config是指上一个使用的config，而不是上一个到来的config。因为Channel的入端已经做了1.5s更新一次的限制，所以configChannel中的config数量并不多，所以每次收到都做一次全询。
所以Client端也要优化，如果是ErrWrongGroup，那么间隔1s就重试就挺好。**X,前述想法不对**
假设有config1中，有GroupA,GroupB,Config2中，GroupC加入，Config3中GroupC离开。而Config2只在GroupB,GroupC中生效了，而GroupA只监听到了config1和Config3，那么GroupC对于GroupA来说完全是一无所知的，所以GroupA没法向GroupC请求Shard转移。

update_1
Group中会每1.5s监听一次config，如果config更新，就把更新的config都请求过来

## Client
Client怎么知道shard的owner是谁呢？
随机向某个机器发出请求，记为Machine1，Machine1如果是，就回复；不是，就给它按照当前的Config指路。如果只到MachineB.有两种情况
- MachineB知道了more-updated Config。MachineB会为Client指路
- MachineB不知道more-updated Config。MachineB就会提供服务。
- MachineB使用一个stale Config(Config100),但是这个Config100没有生效，所以它也没有这个shard，它也不知道从哪取这个Shard

这里只有加一个强限制，就是Machine1中的会保留所有版本的config，如果Machine1需要某个Shard1，它会从最新的向最老的开始试，因为Shard1所属的机器一定正在使用某个版本的config。

- 保证所有的状态变更都通过raft来固定，这样无论leader怎么被干掉，它的follower都可以继续它的任务


**Challenge**:
能不能实现即使是在配置变更时，GroupB在GroupA还没有生效时，依旧保持服务？
这样就是Config中的配置不一定和每个Server里面应该保存它所知Shard控制权信息。

#Update_2
思考的关键问题，在于以下几个方面：
-   Shard转移相关问题
    -   如果GroupA的config100中，迟迟有一个Shard没有被请求，那就证明有另一个Group的config还没有更新到100,继续等待就是
    -   Shard转移的RPC 
```
//分为RequestShard()和ConfirmReceivedShard()RequestShard
RequestParam{
    GID string//GroupID
    configNum int//configNum
    shardNo int//Shard的ID
}
Reponse{
    shard Shard//
    Err   Err
}

Handler{
    1. configNum是否与此configNum相等，等于才转移。|Shard的状态是否是（停止服务，正在转移）是的话直接把Shard复制一份拿走，否则继续
    2. start一个command
    3. Command成功后，停止Shard服务，并转移Shard
}
ConfirmReceivedShard
RequestParam{
    configNum int
    GID string//GroupID
    shardNo int//Shard的ID
}
```


-   KVServer需要raft来同步何种状态
  在代码里面用√标出
-   config的更新逻辑
    - 维护两个config，一个oldConfig，一个currentConfig。从oldConfig里查询去哪个Group上RequestShard.
    - 每35ms更新一次config，
    ```
    //刚查询到的config记为newConfig，
    if newConfig.num > currentConfig.num
      //请求currentConfig.num+1
    ```
-   Command的处理逻辑
    1. if [is put/get/append]
    这里有可能发现这个Shard已经停止服务了。
    ...
   if [is ShardStateOn]
        //变为【开始服务】
   if [is ShardStateOff] 
        //变为【停止服务，正在转移】
   if [is ShardDelete]
        //删除此Shard
   - 怎么判断该Shard还没有接受到？
   在当前Config里面，Shard的服务状态还没有开始
-   configDaemon的工作流程
```
1.查看config是否还有shard没有接收
  如果没有接收，那么就挨个pull这个Shard
2.查看config是否还有Shard没有发送
   返回
3.去拉取最新的config，如果过于新就再请求currentConfig.Num+1
上述两个步骤保证在config升级时，Group已经升级到lastConfig所指明的状态。如果缺少任意异步
```