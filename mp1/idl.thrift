namespace go demo.rpc

struct EchoRequest {
    1: string msg;
}

struct EchoResponse {
    1: string msg;
}

struct Tx {
    1: string op;
    2: string acct1;
    3: string acct2;
    4: i64 amt;
}

struct DataMsg {
    1: Tx msg;
    2: i64 msgId;
    3: string sender;
}

struct AckMsg {
    1: i64 msgId;
    2: i64 proposedSeq;
}

struct SeqMsg {
    1: i64 msgId;
    2: string sender;
    3: i64 agreedSeq;
    4: string decisionMaker;
}

struct Res {
    1: string deliverTime;
}

service TxService {
    EchoResponse echo(1: EchoRequest req);
	AckMsg sendData(1: DataMsg req);
	Res sendSeq (1: SeqMsg req);
}