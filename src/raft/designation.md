1.这一次log将输入到文件中，准备使用logx
done，没有使用Logx
2.最好画出详细的follower,Candidate,Primary状态转换的状态图。
考虑好就好了，毕竟输入只有appendEntry和requestVote

20221112
1739行 format之后log，从s6发出requestVote到S2收到，最长是经过了100ms,又观察到requestVote等mu.Lock()等了100ms
1789 行 s4和s5同时变为candidate,相差30ms，这个与设计相同 ->更改一下TImout_Interval


