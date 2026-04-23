// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

/*
HelloWorldReadTTL is an example SST application that shows how to read
a turtle file and dumps out the content.

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

	"git.semanticstep.net/x/sst/sst"

	// This fixed import with "_" is essential to initialize the SST dictionary.
	// This need to be manually added.
	_ "git.semanticstep.net/x/sst/vocabularies/dict"
)

func main() {
	// open the "helloworldwrite.ttl" file
	file, err := os.Open("../helloworldwrite/helloworldwrite.ttl")

	// make sure the file is closed before the main function ends
	defer func() {
		e := file.Close()
		if err == nil {
			err = e
		}
	}()

	if err != nil {
		panic("open file failed!")
	}

	// convert the content of the turtle file "helloworldwrite.ttl", which is RDF, to a default NamedGraph.
	st, err := sst.RdfRead(bufio.NewReader(file), sst.RdfFormatTurtle, sst.StrictHandler, sst.DefaultTriplexMode)
	if err != nil {
		log.Panic(err)
	}

	// The following part aims to print out the NamedGraph.
	// The format is as follows:
	// * NamedGraph ID
	// ** First IRInode ID
	// *** Each non-inverse triple of the first IRInode
	// ** Second IRInode ID
	// *** Each non-inverse triple of the Second IRInode
	// ...

	// Here the graph ID is d3871b0e-5c73-4a1f-9fa5-97e2ff403ae4.
	// There is only one IRInode in the graph, the uuid is 8236b0fe-c8b9-4b0c-aa51-158dcd6ba752.
	// For the IRInode 8236b0fe-c8b9-4b0c-aa51-158dcd6ba752, there are two triples:
	// The first triple:
	// * subject: 8236b0fe-c8b9-4b0c-aa51-158dcd6ba752
	// * predicate: rdf:type, IRI: http://www.w3.org/1999/02/22-rdf-syntax-ns#type
	// * object: lci:individual, IRI: http://ontology.semanticstep.net/lci#Individual
	// The second triple:
	// * subject: 8236b0fe-c8b9-4b0c-aa51-158dcd6ba752
	// * predicate: rdfs:label, IRI: http://www.w3.org/2000/01/rdf-schema#label
	// * object: "hello world", IRI of datatype: http://www.w3.org/2001/XMLSchema#string
	// graph := st.NamedGraph(sst.IRI(uuid.MustParse("2f280ec9-f911-43ef-af84-0d49546d97cf").URN()))
	graph := st.NamedGraphs()[0]
	fmt.Printf("Named graph: %s\n", graph.IRI())
	err = graph.ForIRINodes(func(t sst.IBNode) error {
		fmt.Printf("  IBNode: %s\n", t.Fragment())
		return t.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
			if s != t { // skip inverses
				return nil
			}

			// If the object is a IBNode or TermCollection, print out the IRI of its predicate and its object;
			// If the object is a literal, print out the IRI of its predicate, object value and the IRI of object data type.
			// Otherwise, return nil
			switch o.TermKind() {
			case sst.TermKindIBNode, sst.TermKindTermCollection:
				fmt.Printf("    %s %s\n", p.IRI(), o.(sst.IBNode).IRI())
			case sst.TermKindLiteral:
				fmt.Printf("    %s \"%v\"^^%s\n", p.IRI(), o.(sst.Literal), o.(sst.Literal).DataType().IRI())
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
