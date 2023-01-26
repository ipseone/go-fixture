package fixture

import "gonum.org/v1/gonum/graph"

type Record struct {
	nodeID    int64
	nodeLabel [2]string
	nodeFrom  []*Record
	nodeTo    []*Record

	callbacks map[[3]string]func() (interface{}, error)
}

func (r *Record) ID() int64 {
	return r.nodeID
}

func (r *Record) Label() [2]string {
	return r.nodeLabel
}

func (r *Record) AppendTo(node *Record) {
	r.nodeTo = append(r.nodeTo, node)
}

func (r *Record) AppendFrom(node *Record) {
	r.nodeFrom = append(r.nodeFrom, node)
}

func (r *Record) LenTo() int {
	return len(r.nodeTo)
}

func (r *Record) LenFrom() int {
	return len(r.nodeFrom)
}

type Records struct {
	idx int
	l   []*Record
}

func (r *Records) Next() bool {
	if len(r.l) == 0 || r.idx >= len(r.l) {
		return false
	}

	r.idx++

	return true
}

func (r *Records) Len() int {
	return len(r.l) - r.idx
}

func (r *Records) Reset() {
	r.idx = 0
}

func (r *Records) Node() graph.Node {
	return r.l[r.idx-1]
}

type Edge struct {
	to   *Record
	from *Record
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
