#TaskA 20230214
总体设计：
看起来就是使用raft把题目所列出的四个动作排序。

replica_G1:M10,M12,M12->shards:0-99
replica_G2:M20,m22,m21,m23->shards:100-199


##Join
args:
replica_G3:m31,m30,m32 
replica_G4:m40,m41,m42


shardsNum/count(replica_groups)向上取整
比如，shardNum=10,count(replica_group)=200
则应该是50,50,50,50
10/4 = 3
7/3 = 3
4/2 = 2
2/1 = 2

replica平均分配算法
对于每个replica_group，它现有shards记为numShards，要拥有的记为expectNum
if numShards > expectNum 
   就从末尾取出相应数量,标记为空闲shards
if numShards < expectNum
   就从空闲shards首部取出相应的shards，分配给它。

使用一个数组装载现有Gid，即sc.gids，按照添加时间排序。
使用一个gidToShards map[gid][]int:
for idx,gid:=range sc.gids{
   //求出未分配shards的gid数量，记为unAllocateShardsNum
   //expectedNum = UnAllocatedShardNum/unAllocateShardsNum 并向上取整
   
}

rebalance(){
   1.将所有gid按照拥有的shards数量由大到小排序。
   2.expectedNum = UnAllocatedShardNum/unAllocateSigNum 并向上取整
   if numShards > expectNum 
      就从末尾取出相应数量,标记为空闲shards
   if numShards < expectNum
      就从空闲shards首部取出相应的shards，分配给它。
}

Sig1:1 2 3 4 5 -》[5]
Sig2:6 7 8 9 10 -> [9,10] 
Sig3:5,9,10

##Leave
args:
replica_G3
replica_G4

直接去除相应的Gid，然后将空闲的shard分配

##Move
args:
replica_G3
replica_G4

就是move
##Query
就是查询