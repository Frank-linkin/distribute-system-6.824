出现这样一个问题。一个请求已经使用了logIndex=122，超时返回了，但是logIndex=122commit成功了，但是还没有应用到application里面，所以又会给这个请求发一个logIndex=122。详细见log中编号为7e2c1d5d_No.35的请求。

方案1：
记录logIndex和requestID的对应关系。map[requestID]logIndex
1.如果此请求成功返回，就删除对应关系
2.如果此请求达到，发现有对应关系，将请求hold到app更新到这个logIndex。

这个方案是不行的，因为如果机器更换，新的Leader被选出来，它里面没有requestID和LogIndex对应关系，所以又会让这个请求重复。

正确解决方法是，允许一个RequestID使用多个logIndex，但是所有的logIndex都只执行一次。

