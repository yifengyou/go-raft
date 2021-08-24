#!/bin/bash

set -xe

NUM=3
if [ ! -f kvstore ];then
	echo "kvstore not found!"
	exit 1
fi

if [ ! -f ../../cmd/raftctl/raftctl ];then
	echo "raftctl not found!"
	exit 1
fi

if [ -d workdir ];then
	rm -rf workdir
fi
mkdir workdir
for ((i=1; i<=${NUM}; i ++));do
	export CID=1234
	export NID=${i} 
	echo "start node-${i}"
	echo ""
	echo "./kvstore workdir/data${i} localhost:$((7000+$i)) localhost:$((8000+$i))"
	echo ""
	screen -L workdir/log.${i} -dmS node-${i}  ./kvstore workdir/data${i} localhost:$((7000+$i)) localhost:$((8000+$i))
done

screen -list
sync
sleep 1

cd ../../cmd/raftctl/
export RAFT_ADDR=localhost:7001
CMD="./raftctl config apply" 
for ((i=1; i<=${NUM}; i ++));do
	CMD="${CMD} +nid=${i},voter=true,addr=localhost:$((7000+$i)),data=localhost:$((8000+$i)) "
done
echo "run cmd: ${CMD}"

pwd
echo $RAFT_ADDR
${CMD}

./raftctl config get


echo "All done!"
