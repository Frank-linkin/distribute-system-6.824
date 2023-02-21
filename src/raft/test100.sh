#TODO:将脚本逻辑改成
#分别执行每个脚本1000次，如果出错，就给这个Log一个序号并显示
function execute() {
    count=1
    while(( $count<=100 ))   
    do
        echo ${count}' times try'
        rm -rf /golang_space/6.824/src/raft/$2
        VERBOSE=1 go test -run $1
        result=$?

        if(( $result != 0 ))
        then
            echo "出错啦,failCount="$failCount
            mv /golang_space/6.824/src/raft/$2 /golang_space/6.824/src/raft/${2}_${failCount}
            let "failCount++"
        fi
        let "count++"
    done
}

function executeTestSeries(){
    echo $1 $2 $3 $4
    arr=$1 #使用数组作为参数，要先把数组赋值给一个变量，然后才能对数组操作

}

result=0
failCount=1
names_2D=("SnapshotBasic2D" "SnapshotInstall2D" "SnapshotInstallUnreliable2D" "SnapshotInstallCrash2D" "SnapshotInstallUnCrash2D")
names_2C=("Persist12C" "Persist22C" "Persist32C" "Figure82C" "UnreliableAgree2C" "Figure8Unreliable2C" "ReliableChurn2C" "UnreliableChurn2C")

#设置环境变量
LOG_2C_FILE='log_2C'
LOG_2D_FILE='log_2D'
echo LOG_2C_FILE=${LOG_2C_FILE}
echo LOG_2D_FILE=${LOG_2D_FILE}


echo '开始脚本'
if [ $1 == '2D' ]
then 
    for name in ${names_2D[@]}
    do
        echo $name
        execute $name ${LOG_2D_FILE}
    done
elif [ $1 == '2C' ]
then
    for name in ${names_2C[@]}
    do
        echo $name
        execute $name ${LOG_2C_FILE}
    done
elif [ $1 == '2CD' ]
then
    for name in ${names_2C[@]}
    do
        echo $name
        execute $name ${LOG_2C_FILE}
    done
    for name in ${names_2D[@]}
    do
        echo $name
        execute $name ${LOG_2D_FILE}
    done
else 
    echo "Wrong params"
fi