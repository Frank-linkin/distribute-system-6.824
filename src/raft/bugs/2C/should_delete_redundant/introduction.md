s0 1 2 3 3 3 3 3 3 3 3 3 3 3 3 3 3
s1 1 2
s2 1 2
此时s0挂掉，s2接替,如果不把redundant logEntry删除的话，会产生
s0 1 2 4 3 3 3 3 3 3 3 3 3 3 3 3 3
s1 1 2 4 
s2 1 2 
然后s1挂掉了,s0可能成为leader，这时候它就会把它的所有的3都replicate到
其他server上。如果这个3很长的话，就会在3第一次接受Start(command)请求的时候对性能造成很大影响。