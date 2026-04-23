// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"github.com/google/uuid"
)

var (
	staticStage    = stage{}
	staticRdfGraph = namedGraph{
		stage:          &staticStage,
		baseIRI:        "http://www.w3.org/1999/02/22-rdf-syntax-ns",
		triplexStorage: []triplex{},
		flags:          namedGraphFlags{isReferenced: true},
	}

	staticSSTGraph = namedGraph{
		stage:          &staticStage,
		baseIRI:        "http://ontology.semanticstep.net/framework",
		triplexStorage: []triplex{},
		flags:          namedGraphFlags{isReferenced: true},
	}

	staticXSDGraph = namedGraph{
		stage:          &staticStage,
		baseIRI:        "http://www.w3.org/2001/XMLSchema",
		triplexStorage: []triplex{},
		flags:          namedGraphFlags{isReferenced: true},
	}

	literalResourceType = ibNodeString{
		fragment: "literal",
		ibNode: ibNode{
			flags: iriNodeType | stringNode,
			ng:    &staticSSTGraph,
		},
	}
	termCollectionResourceType = ibNodeString{
		fragment: "termCollection",
		ibNode: ibNode{
			flags: iriNodeType | stringNode,
			ng:    &staticSSTGraph,
		},
	}
	literalCollectionResourceType = ibNodeString{
		fragment: "literalCollection",
		ibNode: ibNode{
			flags: iriNodeType | stringNode,
			ng:    &staticSSTGraph,
		},
	}
	rdfFirstProperty = ibNodeString{
		fragment: "first",
		ibNode: ibNode{
			flags: iriNodeType | stringNode,
			ng:    &staticRdfGraph,
		},
	}

	// nextNodeProperty    = ibNode{fragment: "http://ontology.semanticstep.net/framework#nextIBNode"}

	literalTypeString = ibNodeString{
		fragment: "string",
		ibNode: ibNode{
			typedResource: typedResource{typeOf: &literalResourceType.ibNode},
			flags:         iriNodeType | stringNode,
			ng:            &staticXSDGraph,
		},
	}

	literalTypeByte = ibNodeString{
		fragment: "byte",
		ibNode: ibNode{
			typedResource: typedResource{typeOf: &literalResourceType.ibNode},
			flags:         iriNodeType | stringNode,
			ng:            &staticXSDGraph,
		},
	}

	literalTypeShort = ibNodeString{
		fragment: "short",
		ibNode: ibNode{
			typedResource: typedResource{typeOf: &literalResourceType.ibNode},
			flags:         iriNodeType | stringNode,
			ng:            &staticXSDGraph,
		},
	}

	literalTypeInt = ibNodeString{
		fragment: "int",
		ibNode: ibNode{
			typedResource: typedResource{typeOf: &literalResourceType.ibNode},
			flags:         iriNodeType | stringNode,
			ng:            &staticXSDGraph,
		},
	}

	literalTypeLong = ibNodeString{
		fragment: "long",
		ibNode: ibNode{
			typedResource: typedResource{typeOf: &literalResourceType.ibNode},
			flags:         iriNodeType | stringNode,
			ng:            &staticXSDGraph,
		},
	}

	literalTypeUnsignedByte = ibNodeString{
		fragment: "unsignedByte",
		ibNode: ibNode{
			typedResource: typedResource{typeOf: &literalResourceType.ibNode},
			flags:         iriNodeType | stringNode,
			ng:            &staticXSDGraph,
		},
	}

	literalTypeUnsignedShort = ibNodeString{
		fragment: "unsignedShort",
		ibNode: ibNode{
			typedResource: typedResource{typeOf: &literalResourceType.ibNode},
			flags:         iriNodeType | stringNode,
			ng:            &staticXSDGraph,
		},
	}

	literalTypeUnsignedInt = ibNodeString{
		fragment: "unsignedInt",
		ibNode: ibNode{
			typedResource: typedResource{typeOf: &literalResourceType.ibNode},
			flags:         iriNodeType | stringNode,
			ng:            &staticXSDGraph,
		},
	}

	literalTypeUnsignedLong = ibNodeString{
		fragment: "unsignedLong",
		ibNode: ibNode{
			typedResource: typedResource{typeOf: &literalResourceType.ibNode},
			flags:         iriNodeType | stringNode,
			ng:            &staticXSDGraph,
		},
	}

	// combine val and langTag into one string, the last 2 bytes are for the langTag.
	// There is no separator between LangString value and langTag.
	literalTypeLangString = ibNodeString{
		fragment: "langString",
		ibNode: ibNode{
			typedResource: typedResource{typeOf: &literalResourceType.ibNode},
			flags:         iriNodeType | stringNode,
			ng:            &staticRdfGraph,
		},
	}

	literalTypeDateTime = ibNodeString{
		fragment: "dateTime",
		ibNode: ibNode{
			typedResource: typedResource{typeOf: &literalResourceType.ibNode},
			flags:         iriNodeType | stringNode,
			ng:            &staticXSDGraph,
		},
	}

	literalTypeDateTimeStamp = ibNodeString{
		fragment: "dateTimeStamp",
		ibNode: ibNode{
			typedResource: typedResource{typeOf: &literalResourceType.ibNode},
			flags:         iriNodeType | stringNode,
			ng:            &staticXSDGraph,
		},
	}

	literalTypeDouble = ibNodeString{
		fragment: "double",
		ibNode: ibNode{
			typedResource: typedResource{typeOf: &literalResourceType.ibNode},
			flags:         iriNodeType | stringNode,
			ng:            &staticXSDGraph,
		},
	}

	literalTypeFloat = ibNodeString{
		fragment: "float",
		ibNode: ibNode{
			typedResource: typedResource{typeOf: &literalResourceType.ibNode},
			flags:         iriNodeType | stringNode,
			ng:            &staticXSDGraph,
		},
	}

	literalTypeInteger = ibNodeString{
		fragment: "integer",
		ibNode: ibNode{
			typedResource: typedResource{typeOf: &literalResourceType.ibNode},
			flags:         iriNodeType | stringNode,
			ng:            &staticXSDGraph,
		},
	}

	literalTypeBoolean = ibNodeString{
		fragment: "boolean",
		ibNode: ibNode{
			typedResource: typedResource{typeOf: &literalResourceType.ibNode},
			flags:         iriNodeType | stringNode,
			ng:            &staticXSDGraph,
		},
	}

	rdfVocabulary = Vocabulary{BaseIRI: "http://www.w3.org/1999/02/22-rdf-syntax-ns"}
	rdfType       = Element{Vocabulary: rdfVocabulary, Name: "type"}

	rdfsVocabulary    = Vocabulary{BaseIRI: "http://www.w3.org/2000/01/rdf-schema"}
	rdfsSubPropertyOf = Element{Vocabulary: rdfsVocabulary, Name: "subPropertyOf"}
	rdfsLabel         = Element{Vocabulary: rdfsVocabulary, Name: "label"}
	rdfsComment       = Element{Vocabulary: rdfsVocabulary, Name: "comment"}

	lciVocabulary   = Vocabulary{BaseIRI: "http://ontology.semanticstep.net/lci"}
	lciOrganization = Element{Vocabulary: lciVocabulary, Name: "Organization"}
	lciPerson       = Element{Vocabulary: lciVocabulary, Name: "Person"}

	ssoVocabulary     = Vocabulary{BaseIRI: "http://ontology.semanticstep.net/sso"}
	ssoPart           = Element{Vocabulary: ssoVocabulary, Name: "Part"}
	ssoAssemblyDesign = Element{Vocabulary: ssoVocabulary, Name: "AssemblyDesign"} //lint:ignore U1000 not used temporary
	ssoIDOwner        = Element{Vocabulary: ssoVocabulary, Name: "idOwner"}        //lint:ignore U1000 not used temporary
	ssoID             = Element{Vocabulary: ssoVocabulary, Name: "id"}             //lint:ignore U1000 not used temporary

	xsdVocabulary = Vocabulary{BaseIRI: "http://www.w3.org/2001/XMLSchema"}

	internIDNamespace = uuid.UUID{0x2e, 0x56, 0x53, 0xc4, 0x34, 0x4b, 0x4d, 0x82, 0x91, 0xbb, 0x98, 0x3b, 0x07, 0x36, 0x1a, 0x6b} // uuid.MustParse("2e5653c4-344b-4d82-91bb-983b07361a6b")
	internIDSelf      = uuid.UUID{0x5c, 0x19, 0xf5, 0x8d, 0x21, 0x9e, 0x5c, 0x77, 0xb1, 0xc7, 0x99, 0xa0, 0x4c, 0x40, 0xb4, 0x66} // uuid.NewSHA1(internIDNamespace, []byte("."))
	internIDMain      = uuid.UUID{}                                                                                               // uuid.Nil
	_, _, _           = internIDNamespace, internIDSelf, internIDMain
	deleteMarker      = ibNode{} // deleteMarker is used by ibNode.forAllTriplexes() as deleted triplex marker
)
