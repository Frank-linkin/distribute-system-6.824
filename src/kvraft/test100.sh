count=1
result=0
echo '开始脚本'

while(( $count<=100 && result == 0 ))   
do
    echo ${count}' times try'
    rm -rf /golang_space/6.824/src/kvraft/log*
    #rm -rf test_result
    go test -run SnapshotUnreliableRecoverConcurrentPartitionLinearizable3B
    result=$?
    let "count++"
done
