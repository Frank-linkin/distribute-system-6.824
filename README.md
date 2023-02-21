##6.824 Lab 4: Sharded Key/Value Service

这是[MIT6.824课程的lab项目](https://pdos.csail.mit.edu/6.824/index.html)。

项目的详细架构图如下：
![image](https://github.com/Frank-linkin/distribute-system-6.824/blob/master/IMG/lab4B.png)

- ShardCtrler：负责Config的管理与分发，所有的Group都需要与ShardCtrl对话来获取Config。每此往集群里面加入一个新的Group(集群)，从集群里面删除一个Group，都会生成一个新版本的Config。集群监听到新的版本的Config后会进行相应的变化。
- ShardKV：建立在多组计算机上的线性一致性键值存储数据库。系统保证在多个Client并发通信的时候的线性一致性，并保证在集群的配置变更不会对外部用户不可见。
- Group：基于Raft协议的计算机组，保证少数结点宕机依然可以提供稳定的对外服务。每个Group负责管理一组Shard，当Group离开集群时，需要将Shard移交给其他Group来管理。

本项目已经通过6.824全部测试。
