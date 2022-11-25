
select_exit_and_ticker
1.select中exit和timeTicker在一起的话，可能会一直选ticker而不exit。有时候可能一分钟内选的都是ticker。heartbeatInterval是40ms

paper_5.4.1_restriction
目前配置：
Leader刚上位的时候，follower不会进行back，待AE的prevLogTerm==Leader.currentTerm的时候，才开始backup。backup的规则是：
利用Leader存有matchIdx，根据所有follower的match idx，确定当前的commitIdx。
这其实看似遵守了5.4.1，但隐含还是违背了。下面例子中，初始状态为：
S1 1 2 2 2 3 3 3
s2 1
s3 1
之后S1选为Leader，并接到一个请求
   1 2 3 4 5 6 7 8
S1 1 2 2 2 3 3 3 4
S2 1 2 2 2
s3 1 2 2 2 3 3 3 
这时候会把commitIndex提高到4，显然是不符合5.4.1，因为Term=4的数据并没有被commit成功。

easy_to_deadLock
在某个Lock的临界区调用一个有锁的方法是非常危险的，一般是在临界区把需要的信息提取出来。不然很容易发生死锁。
