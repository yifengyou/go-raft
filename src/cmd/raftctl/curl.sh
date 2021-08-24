#!/bin/bash

set -x

HOST=192.168.1.199

#curl -v -X POST ${HOST}:8001/k1 -d v1
curl -v -X POST ${HOST}:8001/k1 -d youyifeng
curl -v ${HOST}:8001/k1
curl -v -L ${HOST}:8002/k1
curl -v "${HOST}:8002/k1?dirty"

echo ""
i=1
set +x
while true;do
	i=$(($i+1))
	#echo "========================================"
	curl -v -X POST ${HOST}:8001/k1 -d you${i} &>/dev/null &
	#echo "ret:"`curl -L ${HOST}:8001/k1`
	#echo "ret:"`curl -L ${HOST}:8002/k1`
	#echo "ret:"`curl -L ${HOST}:8003/k1`
	#sleep 1
done
