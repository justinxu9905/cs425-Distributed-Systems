package multicast

type QueueElem struct {
	Op        string
	Acct1     string
	Acct2     string
	Amt       int64
	MsgId     int64
	Sender    string
	Proposer  string
	Seq       int64
	Delivered bool
}
