// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst_test

import (
	"fmt"
	"log"

	"git.semanticstep.net/x/sst/sst"
)

func simpleNamedGraph() sst.NamedGraph {
	st := sst.OpenStage(sst.DefaultTriplexMode)
	g := st.CreateNamedGraph("")
	return g
}

func Example_simple() {
	g := simpleNamedGraph()

	hasPart := g.CreateIRINode("http//semanticstep.net/sst#hasPart")
	description := g.CreateIRINode("http//semanticstep.net/sst#description")
	t1 := g.CreateIRINode("main")
	t2 := g.CreateBlankNode()

	t1.AddStatement(description, sst.String("t1"))
	t1.AddStatement(hasPart, t2)
	if err := t1.ForAll(func(index int, s, p sst.IBNode, o sst.Term) error {
		switch o.TermKind() {
		case sst.TermKindIBNode, sst.TermKindTermCollection:
			o := o.(sst.IBNode)
			fmt.Printf("%d %v %v %v\n", index, s.Fragment(), p == hasPart, o == t2)
		case sst.TermKindLiteral:
			fmt.Printf("%d %v %v %v\n", index, s.Fragment(), p == description, o.(sst.Literal))
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}
	// Output:
	// 0 main true t1
	// 1 main true true
}
