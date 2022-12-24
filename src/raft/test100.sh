#TODO:将脚本逻辑改成
#分别执行每个脚本1000次，如果出错，就给这个Log一个序号并显示
function execute() {
    count=1
    while(( $count<=200 ))   
    do
        echo ${count}' times try'
        rm -rf /golang_space/6.824/src/raft/log
        VERBOSE=1 go test -run $1
        result=$?

        if(( $result != 0 ))
        then
            echo "出错啦,failCount="$failCount
            mv /golang_space/6.824/src/raft/log /golang_space/6.824/src/raft/log_$failCount
            let "failCount++"
        fi
        let "count++"
    done
}

result=0
failCount=1
strs_2D=("SnapshotBasic2D" "SnapshotInstall2D" "SnapshotInstallUnreliable2D" "SnapshotInstallCrash2D" "SnapshotInstallUnCrash2D")

echo '开始脚本'

for name in ${strs_2D[@]} 
do
    echo $name
    execute $name
done