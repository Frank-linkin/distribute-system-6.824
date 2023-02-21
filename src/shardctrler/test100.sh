count=1
result=0
echo '开始脚本'

while(( $count<=1000 && result == 0 ))   
do
    echo ${count}' times try'
    rm -rf /golang_space/6.824/src/kvraft/log*
    #rm -rf test_result
    VERBOSE=1 go test 
    result=$?
    let "count++"
done
