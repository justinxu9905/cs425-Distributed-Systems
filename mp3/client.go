package main

import (
	"bufio"
	"context"
	"cs-425-mp3/common"
	"cs-425-mp3/gen-go/transaction/rpc"
	"cs-425-mp3/handler"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

const ArgNumClient int = 2

func main() {
	// parse arguments
	argv := os.Args[1:]
	if len(argv) != ArgNumClient {
		log.Println("Usage: ./mp3_node <client identifier> <configuration file>")
		os.Exit(1)
	}
	clientId := argv[0]
	cfgFilePath := argv[1]

	// parse configure file
	nodes := common.ParseCfgFile(cfgFilePath)

	// run transaction client
	cliMap := map[string]*rpc.TransactionServiceClient{}
	swchMap := map[string]*thrift.TSocket{}
	for node, nodeInfo := range nodes {
		cliMap[node], swchMap[node] = handler.NewTransactionServiceCli(nodeInfo)
	}

	nodeGroup := map[string]*rpc.TransactionServiceClient{}
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
					nodeGroup[node] = cliMap[node]
				}
			}
			time.Sleep(time.Second)
		}
		done <- true
	}()
	<-done

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		input := scanner.Text()
		if input == "BEGIN" {
			cli := randomClient(nodeGroup)
			connReq := &rpc.ConnectRequest{
				ClientId: clientId,
			}
			connResp, _ := cli.Connect(context.Background(), connReq)
			if connResp.Ok {
				fmt.Println("OK")
			} else {
				continue
			}

			req := &rpc.PrepareRequest{
				ClientId: clientId,
				Op: &rpc.Operation{
					Op: input,
				},
			}
			cli.Prepare(context.Background(), req)

			for scanner.Scan() {
				input = scanner.Text()

				if strings.Split(input, " ")[0] != "DEPOSIT" && strings.Split(input, " ")[0] != "WITHDRAW" && strings.Split(input, " ")[0] != "BALANCE" && strings.Split(input, " ")[0] != "COMMIT" && strings.Split(input, " ")[0] != "ABORT" {
					continue
				}

				var acct string
				var amt int64
				if strings.Split(input, " ")[0] == "BALANCE" {
					acct = strings.Split(input, " ")[1]
					amt = 0
				} else if strings.Split(input, " ")[0] == "COMMIT" || strings.Split(input, " ")[0] == "ABORT" {
					acct = ""
					amt = 0
				} else {
					acct = strings.Split(input, " ")[1]
					amt, _ = strconv.ParseInt(strings.Split(input, " ")[2], 10, 32)
				}

				req = &rpc.PrepareRequest{
					ClientId: clientId,
					Op: &rpc.Operation{
						Op: strings.Split(input, " ")[0],
						Acct: acct,
						Amt: int32(amt),
					},
				}
				resp, _ := cli.Prepare(context.Background(), req)
				if !resp.Ok {
					fmt.Println(resp.Msg)
					break
				}

				if strings.Split(input, " ")[0] == "COMMIT" {
					cfmReq := &rpc.ConfirmRequest{
						ClientId: clientId,
						Commit: true,
					}
					cfmResp, _ := cli.Confirm(context.Background(), cfmReq)
					if !cfmResp.Ok {
						fmt.Println("ABORTED")
						break
					}
					fmt.Println("COMMIT OK")
					break
				}

				fmt.Println(resp.Msg)
			}
		}
	}
}

func handleInput() string {
	/*scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	input := scanner.Text()*/

	/*reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter text: ")
	input, _ := reader.ReadString('\n')*/

	var input string
	_, _ = fmt.Scanln(&input)
	if len(input) == 0 {
		return ""
	}
	//fmt.Println(input + "!")
	return input
}

func randomClient(clientGroup map[string]*rpc.TransactionServiceClient) *rpc.TransactionServiceClient {
	i := rand.Intn(len(clientGroup))
	c := 0
	var dft *rpc.TransactionServiceClient
	for _, v := range clientGroup {
		if i == c {
			return v
		}
		dft = v
	}
	return dft
}