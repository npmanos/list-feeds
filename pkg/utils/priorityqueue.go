// Adapted from: https://pkg.go.dev/container/heap#example-package-PriorityQueue
package utils

import (
	"container/heap"
	"log"
)


type queueItem[E comparable] struct {
	value E
	priority int
	index int
}

type PriorityQueue[E comparable] []*queueItem[E]

func NewPriorityQueue[E comparable](itemsPriority map[E]int) *PriorityQueue[E] {
	pq := make(PriorityQueue[E], len(itemsPriority))
	var i = 0
	for value, priority := range itemsPriority {
		pq[i] = &queueItem[E]{
			value: value,
			priority: priority,
			index: i,
		}
		i++
	}

	 heap.Init(&pq)

	 return &pq
}

func (pq PriorityQueue[E]) Len() int { return len(pq) }

func (pq PriorityQueue[E]) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority
}

func (pq PriorityQueue[E]) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue[E]) Push(e any) {
	item, ok := e.(*queueItem[E])
	if !ok {
		log.Fatalf("pushed item %v is not type *queueItem[E]. Did you mean PushTyped()?", e)
	}

	n := len(*pq)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue[E]) PushTyped(e E, priority int) {
	heap.Push(pq, &queueItem[E]{value: e, priority: priority})
}

func (pq *PriorityQueue[E]) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

func (pq *PriorityQueue[E]) PopTyped() (E, int) {
	item := heap.Pop(pq).(*queueItem[E])
	return item.value, item.priority
}

