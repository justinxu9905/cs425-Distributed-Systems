package handler

import (
	"context"
	"cs-425-mp3/common"
	"cs-425-mp3/gen-go/transaction/rpc"
	"cs-425-mp3/transaction"
	"github.com/apache/thrift/lib/go/thrift"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

type TransactionHandler struct {
	Mgr *transaction.TransactionMgr
	ClientTimestampMap map[string]int64
	NodeGroup map[string]*rpc.TransactionServiceClient
}

func (t TransactionHandler) Begin(ctx context.Context, req *rpc.BeginRequest) (_r *rpc.BeginResponse, _err error) {
	resp := &rpc.BeginResponse{
		Timestamp: t.Mgr.Begin(),
	}
	t.ClientTimestampMap[req.ClientId] = resp.Timestamp
	return resp, nil
}

func (t TransactionHandler) Execute(ctx context.Context, req *rpc.ExecuteRequest) (_r *rpc.ExecuteResponse, _err error) {
	resp := &rpc.ExecuteResponse{}
	err, res := t.Mgr.Execute(req.Timestamp, transaction.Operation{
		Op: req.Op.Op,
		Acct: req.Op.Acct,
		Amt: int(req.Op.Amt),
	})
	if err == nil {
		resp.Ok = true
		if req.Op.Op == "BALANCE" {
			resp.Msg = req.Op.Acct + " = " + strconv.Itoa(res)
		} else {
			resp.Msg = "OK"
		}
	} else if err == common.ErrorNotFound {
		resp.Ok = false
		resp.Msg = "NOT FOUND, ABORTED"
	} else if err == common.ErrorIsolationViolated {
		resp.Ok = false
		resp.Msg = "ABORTED"
	}
	return resp, nil
}

func (t TransactionHandler) Committable(ctx context.Context, req *rpc.CommittableRequest) (_r *rpc.CommittableResponse, _err error) {
	//fmt.Printf("[Committable Handler] try to check %v\n", req.Timestamp)
	resp := &rpc.CommittableResponse{
		Ok: t.Mgr.Committable(req.Timestamp),
	}
	return resp, nil
}

func (t TransactionHandler) Commit(ctx context.Context, req *rpc.CommitRequest) (_r *rpc.CommitResponse, _err error) {
	//fmt.Printf("[Commit Handler] try to commit %v\n", req.Timestamp)
	resp := &rpc.CommitResponse{
		Ok: t.Mgr.Commit(req.Timestamp),
	}
	return resp, nil
}

func (t TransactionHandler) Abort(ctx context.Context, req *rpc.AbortRequest) (_r *rpc.AbortResponse, _err error) {
	resp := &rpc.AbortResponse{Ok: true}
	t.Mgr.Abort(req.Timestamp)
	return resp, nil
}

func (t TransactionHandler) Connect(ctx context.Context, req *rpc.ConnectRequest) (_r *rpc.ConnectResponse, _err error) {
	resp := &rpc.ConnectResponse{
		Ok: true,
	}
	//fmt.Printf("[Connect Handler] connect with client %v\n", req.ClientId)
	return resp, nil
}

func (t TransactionHandler) Prepare(ctx context.Context, req *rpc.PrepareRequest) (_r *rpc.PrepareResponse, _err error) {
	//fmt.Printf("[Prepare Handler] try to execute %v\n", req.Op)
	resp := &rpc.PrepareResponse{}
	switch req.Op.Op {
	case "BEGIN":
		for _, node := range t.NodeGroup {
			node.Begin(ctx, &rpc.BeginRequest{
				ClientId: req.ClientId,
			})
		}
		resp.Ok = true
		resp.Msg = "OK"
		break
	case "DEPOSIT", "WITHDRAW", "BALANCE":
		svrId := strings.Split(req.Op.Acct, ".")[0]
		if _, ok := t.NodeGroup[svrId]; !ok {
			for _, node := range t.NodeGroup {
				_, _ = node.Abort(ctx, &rpc.AbortRequest{
					Timestamp: t.ClientTimestampMap[req.ClientId],
				})
			}
			resp.Ok = false
			resp.Msg = "NOT FOUND, ABORTED"
			break
		}
		r, _ := t.NodeGroup[svrId].Execute(ctx, &rpc.ExecuteRequest{
			Timestamp: t.ClientTimestampMap[req.ClientId],
			Op: req.Op,
		})
		if !r.Ok {
			for _, node := range t.NodeGroup {
				_, _ = node.Abort(ctx, &rpc.AbortRequest{
					Timestamp: t.ClientTimestampMap[req.ClientId],
				})
			}
		}
		resp.Ok = r.Ok
		resp.Msg = r.Msg
		break
	case "COMMIT":
		resp.Ok = true
		resp.Msg = "COMMIT OK"
		for _, node := range t.NodeGroup {
			//fmt.Printf("try %v commitable\n", id)
			r, _ := node.Committable(ctx, &rpc.CommittableRequest{
				Timestamp: t.ClientTimestampMap[req.ClientId],
			})
			//fmt.Printf("%v commitable finished\n", id)
			if !r.Ok {
				resp.Ok = r.Ok
				resp.Msg = "ABORTED"
				break
			}
		}
		if !resp.Ok {
			for _, node := range t.NodeGroup {
				_, _ = node.Abort(ctx, &rpc.AbortRequest{
					Timestamp: t.ClientTimestampMap[req.ClientId],
				})
			}
		}
		break
	case "ABORT":
		resp.Ok = false
		resp.Msg = "ABORTED"
		for _, node := range t.NodeGroup {
			_, _ = node.Abort(ctx, &rpc.AbortRequest{
				Timestamp: t.ClientTimestampMap[req.ClientId],
			})
		}
	}
	return resp, nil
}

func (t TransactionHandler) Confirm(ctx context.Context, req *rpc.ConfirmRequest) (_r *rpc.ConfirmResponse, _err error) {
	resp := &rpc.ConfirmResponse{
		Ok: t.Mgr.Commit(t.ClientTimestampMap[req.ClientId]),
	}
	for _, node := range t.NodeGroup {
		r, _ := node.Commit(ctx, &rpc.CommitRequest{
			Timestamp: t.ClientTimestampMap[req.ClientId],
		})
		//fmt.Printf("%v commit finished\n", id)
		if !r.Ok {
			resp.Ok = r.Ok
			break
		}
	}
	return resp, nil
}

func NewTransactionServiceSvr(nodeInfo common.NodeInfo, txMgr *transaction.TransactionMgr, nodeGroup map[string]*rpc.TransactionServiceClient) *thrift.TSimpleServer {
	port := strconv.Itoa(nodeInfo.Port)
	transport, err := thrift.NewTServerSocket(":" + port)
	if err != nil {
		panic(err)
	}

	handler := &TransactionHandler{
		Mgr: txMgr,
		ClientTimestampMap: make(map[string]int64),
		NodeGroup: nodeGroup,
	}
	processor := rpc.NewTransactionServiceProcessor(handler)

	transportFactory := thrift.NewTTransportFactory()
	protocolFactory := thrift.NewTCompactProtocolFactory()
	server := thrift.NewTSimpleServer4(
		processor,
		transport,
		transportFactory,
		protocolFactory,
	)
	return server
}

func NewTransactionServiceCli(nodeInfo common.NodeInfo) (*rpc.TransactionServiceClient, *thrift.TSocket) {
	ip, err := net.LookupHost(nodeInfo.Addr)
	if err != nil {
		log.Println("DNS analysing for node addr went wrong")
		os.Exit(1)
	}
	port := strconv.Itoa(nodeInfo.Port)
	transport, err := thrift.NewTSocket(net.JoinHostPort(ip[0], port))
	if err != nil {
		panic(err)
	}

	transportFactory := thrift.NewTTransportFactory()
	protocolFactory := thrift.NewTCompactProtocolFactory()

	useTransport, err := transportFactory.GetTransport(transport)
	client := rpc.NewTransactionServiceClientFactory(useTransport, protocolFactory)
	return client, transport
}