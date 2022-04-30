package multicast

import (
	"container/heap"
	"fmt"
	"testing"
)

func TestQueuePush(t *testing.T) {
	queue := TxHeap{}
	heap.Push(&queue, &QueueElem{
		Seq:      1,
		Proposer: "A",
	})
	heap.Push(&queue, &QueueElem{
		Seq:      3,
		Proposer: "A",
	})
	heap.Push(&queue, &QueueElem{
		Seq:      2,
		Proposer: "B",
	})
	heap.Push(&queue, &QueueElem{
		Seq:      2,
		Proposer: "A",
	})
	fmt.Println(queue.Top())
	for _, e := range queue {
		fmt.Println(e)
	}
}

func TestQueuePop(t *testing.T) {
	queue := TxHeap{}
	heap.Push(&queue, &QueueElem{
		Seq:      1,
		Proposer: "A",
	})
	heap.Push(&queue, &QueueElem{
		Seq:      3,
		Proposer: "A",
	})
	heap.Push(&queue, &QueueElem{
		Seq:      2,
		Proposer: "B",
	})
	heap.Push(&queue, &QueueElem{
		Seq:      2,
		Proposer: "A",
	})
	fmt.Println(heap.Pop(&queue))
	fmt.Println(heap.Pop(&queue))
	fmt.Println(heap.Pop(&queue))
	fmt.Println(heap.Pop(&queue))
}

func TestQueueSort(t *testing.T) {
	queue := TxHeap{}
	heap.Push(&queue, &QueueElem{
		Seq:      1,
		Proposer: "A",
	})
	heap.Push(&queue, &QueueElem{
		Seq:      3,
		Proposer: "A",
	})
	heap.Push(&queue, &QueueElem{
		Seq:      2,
		Proposer: "B",
	})
	heap.Push(&queue, &QueueElem{
		Seq:      2,
		Proposer: "A",
	})
	for _, e := range queue {
		fmt.Println(e)
	}
	queue[3].Seq = 5
	heap.Fix(&queue, queue.Len()-1)
	for _, e := range queue {
		fmt.Println(e)
	}
}
