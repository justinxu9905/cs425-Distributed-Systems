package multicast

//max heap by default
type TxHeap []*QueueElem

func (h TxHeap) Len() int { return len(h) }
func (h TxHeap) Less(i, j int) bool {
	if h[i].Seq != h[j].Seq {
		return h[i].Seq < h[j].Seq
	} else {
		return h[i].Proposer < h[j].Proposer
	}
}
func (h TxHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *TxHeap) Pop() interface{} {
	old := *h
	n := h.Len()
	if n == 0 {
		return nil
	}
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *TxHeap) Top() (interface{}, bool) {
	n := h.Len()
	if n == 0 {
		return nil, false
	}
	return (*h)[0], true
}

func (h *TxHeap) Push(x interface{}) {
	*h = append(*h, x.(*QueueElem))
}
