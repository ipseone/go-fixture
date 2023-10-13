package fixture

import "gonum.org/v1/gonum/graph"

type Node struct {
	id    int64
	label [2]string
	from  []*Node
	to    []*Node

	callbacks []func() error
}

func (r *Node) ID() int64 {
	return r.id
}

func (r *Node) Label() [2]string {
	return r.label
}

func (r *Node) AppendTo(node *Node) {
	r.to = append(r.to, node)
}

func (r *Node) AppendFrom(node *Node) {
	r.from = append(r.from, node)
}

func (r *Node) LenTo() int {
	return len(r.to)
}

func (r *Node) LenFrom() int {
	return len(r.from)
}

type Nodes struct {
	idx int
	l   []*Node
}

func (r *Nodes) Next() bool {
	if len(r.l) == 0 || r.idx >= len(r.l) {
		return false
	}

	r.idx++

	return true
}

func (r *Nodes) Len() int {
	return len(r.l) - r.idx
}

func (r *Nodes) Reset() {
	r.idx = 0
}

func (r *Nodes) Node() graph.Node {
	return r.l[r.idx-1]
}

type Edge struct {
	to   *Node
	from *Node
}

func (e *Edge) From() graph.Node {
	return e.from
}

func (e *Edge) To() graph.Node {
	return e.to
}

func (e *Edge) ReversedEdge() graph.Edge {
	return &Edge{
		to:   e.from,
		from: e.to,
	}
}
