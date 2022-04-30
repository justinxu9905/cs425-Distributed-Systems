#!/bin/bash

code_folder='src/'
curr_folder=$(pwd)'/'
servers=(A B C D E)
ports=(10001 10002 10003 10004 10005)

# shutdown servers
shutdown() {
	for port in ${ports[@]}; do
		kill -15 $(lsof -ti:$port)
	done
}
trap shutdown EXIT

# compile server & client
go build -o src/client ../client.go
go build -o src/server ../server.go

# initialize servers
cp config.txt ${code_folder}/config.txt
cd $code_folder
for server in ${servers[@]}; do
	./server $server config.txt > ../server_${server}.log 2>&1 &
done

# run 3 tests
./client a config.txt < ${curr_folder}input1.txt > ${curr_folder}output1.log 2>&1
./client a config.txt < ${curr_folder}input2.txt > ${curr_folder}output2.log 2>&1
./client a config.txt < ${curr_folder}input3.txt > ${curr_folder}output3.log 2>&1
./client a config.txt < ${curr_folder}input4.txt > ${curr_folder}output4.log 2>&1

cd $curr_folder
echo "Difference between your output and expected output:"
diff output1.log expected1.txt
diff output2.log expected2.txt
diff output3.log expected3.txt
diff output4.log expected4.txt
