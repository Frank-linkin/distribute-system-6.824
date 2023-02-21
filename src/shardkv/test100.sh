count=1
result=0
echo '开始脚本'

while(( $count<=500 && result == 0 ))   
do
    echo ${count}' times try'
    rm -rf /golang_space/6.824/src/shardkv/log*
    #VERBOSE=1 go test -run Unreliable1
    go test -run Unreliable1

    result=$?
    let "count++"
done
