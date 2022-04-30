package main

import (
	"bufio"
	"context"
	"cs-425-mp1/common"
	"cs-425-mp1/gen-go/demo/rpc"
	"cs-425-mp1/handler"
	"cs-425-mp1/multicast"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"io"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"time"
)

const ArgNumServer int = 2

const (
	pprofAddr string = ":7890"
)

func StartHTTPDebuger() {
	pprofHandler := http.NewServeMux()
	pprofHandler.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	server := &http.Server{Addr: pprofAddr, Handler: pprofHandler}
	server.ListenAndServe()
}

func main() {
	// parse arguments
	argv := os.Args[1:]
	if len(argv) != ArgNumServer {
		log.Println("Usage: ./mp1_node <identifier> <configuration file>")
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

	go StartHTTPDebuger()

	toMgr := multicast.NewTOMgr(nodeId)

	// run isis server
	svr := handler.NewTxServiceSvr(nodeInfo, toMgr)
	go func() {
		if err := svr.Serve(); err != nil {
			panic(err)
		}
	}()

	// run isis clients
	cliMap := map[string]*rpc.TxServiceClient{}
	swchMap := map[string]*thrift.TSocket{}
	for node, nodeInfo := range nodes {
		cliMap[node], swchMap[node] = handler.NewTxServiceCli(nodeInfo)
	}

	clientGroup := map[string]*rpc.TxServiceClient{}
	done := make(chan bool)
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
					clientGroup[node] = cliMap[node]
				}
			}
			time.Sleep(time.Second)
		}
		done <- true
	}()
	<-done
	toMgr.SetClientGroup(clientGroup)

	// delete crashed clients asynchronously
	go toMgr.RunTask()

	// multicast transactions
	fn := fmt.Sprintf("%v_%v_output.txt", nodeId, cfgFilePath)
	f, _ := os.Create(fn)
	for {
		_, _ = fmt.Scan()
		input, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if err != nil {
			panic(err)
		}
		startTime := time.Now()
		op, acct1, acct2, amt := common.ParseTx(input)
		req := &multicast.Msg{
			Op:    op,
			Acct1: acct1,
			Acct2: acct2,
			Amt:   amt,
		}
		endTime := toMgr.TOMulticast(context.Background(), req)

		_, _ = io.WriteString(f, fmt.Sprintf("%v\n", endTime.Sub(startTime).Nanoseconds()))
	}
}
