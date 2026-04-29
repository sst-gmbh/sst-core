// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

/*
HelloWorldReadSST is an example SST application that shows how to read
a SST file and dumps out the content.

The resulting output on the console:

Named graph: urn:uuid:b7c02e76-96df-4015-9f07-2a5f6c194d07

	IBNode:
	IBNode: 6dcb8d58-9ac1-4395-91c3-c8b0e72bc4f4
	  http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://ontology.semanticstep.net/lci#Individual
	  http://www.w3.org/2000/01/rdf-schema#label "HelloWorld"^^http://www.w3.org/2001/XMLSchema#string

The first IBNode listed is the default implictly defined IBNode the that represents the whole NamedGraph; this one has no fragment.
The fragment of the second IBNode is a UUID and contains two subject triples.
*/
package main

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/semanticstep/sst-core/sst"
)

func main() {
	in, err := os.Open("../helloworldwrite/helloworldwrite.sst")
	if err != nil {
		log.Panic(err)
	}
	graph, err := sst.SstRead(bufio.NewReader(in), sst.DefaultTriplexMode)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("Named graph: %s\n", graph.IRI())
	err = graph.ForIRINodes(func(t sst.IBNode) error {
		fmt.Printf("  IBNode: %s\n", t.Fragment())
		return t.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
			if s != t { // skip inverses
				return nil
			}
			switch o.TermKind() {
			case sst.TermKindIBNode:
				var ib sst.IBNode
				ib = o.(sst.IBNode)
				if ib.IsBlankNode() {
					fmt.Printf("    %s _:%s\n", p.IRI(), ib.ID())
				} else {
					fmt.Printf("    %s %s\n", p.IRI(), ib.IRI())
				}
			case sst.TermKindTermCollection:
				fmt.Printf("    %s %s\n", p.IRI(), o.(sst.TermCollection).ID())
			case sst.TermKindLiteral:
				fmt.Printf("    %s %q^^%s\n", p.IRI(), o.(sst.Literal), o.(sst.Literal).DataType().IRI())
			default:
				fmt.Printf("    %s %s\n", p.IRI(), o)
			}
			return nil
		})
	})
	if err != nil {
		log.Panic(err)
	}
}
