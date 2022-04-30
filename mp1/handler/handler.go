package handler

import (
	"context"
	"cs-425-mp1/common"
	"cs-425-mp1/gen-go/demo/rpc"
	"cs-425-mp1/multicast"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"log"
	"net"
	"os"
	"strconv"
	"time"
)

type TxHandler struct {
	Mgr *multicast.TOMgr
}

func (inst *TxHandler) SendData(ctx context.Context, req *rpc.DataMsg) (_r *rpc.AckMsg, _err error) {
	resp := &rpc.AckMsg{}
	msg := &multicast.Msg{
		Op:    req.Msg.Op,
		Acct1: req.Msg.Acct1,
		Acct2: req.Msg.Acct2,
		Amt:   req.Msg.Amt,
	}
	seq := inst.Mgr.Receive(msg, req.MsgId, req.Sender)
	resp.MsgId = req.MsgId
	resp.ProposedSeq = seq
	//log.txt.Printf("received tx: %v, reply with proposed seq: %v\n", multicastReq, seq)
	return resp, nil
}

func (inst *TxHandler) SendSeq(ctx context.Context, req *rpc.SeqMsg) (_r *rpc.Res, _err error) {
	resp := &rpc.Res{}
	if inst.Mgr.IsDelivered(req.MsgId, req.Sender) {
		return resp, nil
	}
	inst.Mgr.Deliver(req.MsgId, req.AgreedSeq, req.Sender, req.DecisionMaker)
	resp.DeliverTime = time.Now().Format("2006-01-02 15:04:05")
	go inst.Mgr.BMulticast(ctx, req.MsgId, req.AgreedSeq, req.Sender, req.DecisionMaker)
	//log.txt.Printf("received tx id: %v with final seq: %v\n", req.ID, req.AgreedSeq)
	return resp, nil
}

func (inst *TxHandler) Echo(ctx context.Context, req *rpc.EchoRequest) (_r *rpc.EchoResponse, _err error) {
	fmt.Printf("message from client: %v\n", req.GetMsg())

	resp := &rpc.EchoResponse{
		Msg: "Yeah.",
	}

	return resp, nil
}

func NewTxServiceSvr(nodeInfo common.NodeInfo, toMgr *multicast.TOMgr) *thrift.TSimpleServer {
	port := strconv.Itoa(nodeInfo.Port)
	transport, err := thrift.NewTServerSocket(":" + port)
	if err != nil {
		panic(err)
	}

	handler := &TxHandler{
		Mgr: toMgr,
	}
	processor := rpc.NewTxServiceProcessor(handler)

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

func NewTxServiceCli(nodeInfo common.NodeInfo) (*rpc.TxServiceClient, *thrift.TSocket) {
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
	client := rpc.NewTxServiceClientFactory(useTransport, protocolFactory)
	return client, transport
}
