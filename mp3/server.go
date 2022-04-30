package main

import (
	"cs-425-mp3/common"
	"cs-425-mp3/gen-go/transaction/rpc"
	"cs-425-mp3/handler"
	"cs-425-mp3/transaction"
	"github.com/apache/thrift/lib/go/thrift"
	"log"
	"os"
	"time"
)

const ArgNumServer int = 2

func main() {
	// parse arguments
	argv := os.Args[1:]
	if len(argv) != ArgNumServer {
		log.Println("Usage: ./mp3_node <node identifier> <configuration file>")
		os.Exit(1)
	}
	nodeId := argv[0]
	cfgFilePath := argv[1]

	// parse configure file
	nodes := common.ParseCfgFile(cfgFilePath)
	nodeInfo, ok := nodes[nodeId]
	if !ok {
		log.Println("Invalid node identifier")
		os.Exit(1)
	}

	toMgr := transaction.NewTransactionMgr()

	// run transaction client
	cliMap := map[string]*rpc.TransactionServiceClient{}
	swchMap := map[string]*thrift.TSocket{}
	for node, nodeInfo := range nodes {
		cliMap[node], swchMap[node] = handler.NewTransactionServiceCli(nodeInfo)
	}

	nodeGroup := map[string]*rpc.TransactionServiceClient{}
	go func() {
		flag := 1
		for flag != 0 {
			flag = 0
			for node := range cliMap {
				swch := swchMap[node]
				if swch.IsOpen() {
					continue
				}
				if err := swch.Open(); err != nil {
					flag = 1
				}
				if swch.IsOpen() {
					nodeGroup[node] = cliMap[node]
				}
			}
			time.Sleep(time.Second)
		}
	}()

	// run transaction server
	svr := handler.NewTransactionServiceSvr(nodeInfo, toMgr, nodeGroup)
	if err := svr.Serve(); err != nil {
		panic(err)
	}
}
