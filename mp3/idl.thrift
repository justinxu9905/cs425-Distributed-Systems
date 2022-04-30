namespace go transaction.rpc

struct Operation {
    1: string op;
    2: string acct;
    3: i32 amt;
}

struct ConnectRequest {
    1: string clientId;
}

struct ConnectResponse {
    1: bool ok;
}

struct PrepareRequest {
    1: string clientId;
    2: Operation op;
}

struct PrepareResponse {
    1: bool ok;
    2: string msg;
}

struct ConfirmRequest {
    1: string clientId;
    2: bool commit;
}

struct ConfirmResponse {
    1: bool ok;
}


struct BeginRequest {
    1: string clientId;
}

struct BeginResponse {
    1: i64 timestamp;
}

struct ExecuteRequest {
    1: i64 timestamp;
    2: Operation op;
}

struct ExecuteResponse {
    1: bool ok;
    2: string msg;
}

struct CommittableRequest {
    1: i64 timestamp;
}

struct CommittableResponse {
    1: bool ok;
}

struct CommitRequest {
    1: i64 timestamp;
}

struct CommitResponse {
    1: bool ok;
}

struct AbortRequest {
    1: i64 timestamp;
}

struct AbortResponse {
    1: bool ok;
}

service TransactionService {
    ConnectResponse Connect(1: ConnectRequest req);
	PrepareResponse Prepare(1: PrepareRequest req);
	ConfirmResponse Confirm(1: ConfirmRequest req);
	BeginResponse Begin(1: BeginRequest req);
	ExecuteResponse Execute(1: ExecuteRequest req);
	CommittableResponse Committable(1: CommittableRequest req);
	CommitResponse Commit(1: CommitRequest req);
	AbortResponse Abort(1: AbortRequest req);
}