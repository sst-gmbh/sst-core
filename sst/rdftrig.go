// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"io"
)

// trigReader parses TriG documents (RDF 1.1 TriG - extension of Turtle with named graphs).
// TriG allows multiple named graphs to be serialized in a single document using the syntax:
//
//	@prefix ex: <http://example.org/> .
//	
//	ex:graph1 {
//	    ex:subject ex:predicate ex:object .
//	}
//	
//	ex:graph2 {
//	    ex:subject2 ex:predicate2 ex:object2 .
//	}
type trigReader struct {
	*ttlReader
	currentGraph       IRI   // The current named graph being parsed (empty for default graph)
	graphForNextTriple IRI   // The graph context for the next triple to be emitted
	graphStack         []IRI // Stack of graph contexts for triples in the pipeline
}

// newTriGReader creates a new TriG reader that wraps the TTL reader with TriG-specific handling.
func newTriGReader(r io.Reader) *trigReader {
	return &trigReader{
		ttlReader: newTTLReader(r),
	}
}

// read parses a TriG document and returns the next valid triple, or an error.
// It extends the TTL reader by handling named graph blocks.
// Note: The graph context is tracked internally and applied to the stage during RdfRead.
func (d *trigReader) read() (t rdfTriple, err error) {
	defer d.recover(&err)

	// Check if there is already a triple in the pipeline:
	if len(d.triples) >= 1 {
		goto done
	}

	// Return io.EOF when there is no more tokens to parse.
	if d.next().typ == tokenEOF {
		return t, io.EOF
	}
	d.backup()

	// Run the parser state machine.
	// Capture the graph context for any triples that get emitted.
	for d.state = d.parseStartTriG; d.state != nil; {
		// Record how many triples we have before the state runs
		triplesBefore := len(d.triples)
		// Record the current graph context
		graphContext := d.currentGraph
		
		d.state = d.state(d.ttlReader)
		
		// Associate any newly emitted triples with the captured graph context
		triplesAfter := len(d.triples)
		for i := triplesBefore; i < triplesAfter; i++ {
			d.graphStack = append(d.graphStack, graphContext)
		}
	}

	if len(d.triples) == 0 {
		// No triples to emit, i.e only comments and possibly directives was parsed.
		return t, io.EOF
	}
done:
	t = d.triples[0]
	d.triples = d.triples[1:]
	// Pop the graph context for this triple from the stack
	if len(d.graphStack) > 0 {
		d.graphForNextTriple = d.graphStack[0]
		d.graphStack = d.graphStack[1:]
	} else {
		d.graphForNextTriple = ""
	}
	return t, err
}

// parseStartTriG is the TriG entry point that handles directives and graph blocks.
func (d *trigReader) parseStartTriG(t *ttlReader) parseFn {
	tok := t.next()
	
	switch tok.typ {
	case tokenPrefix, tokenSparqlPrefix, tokenBase, tokenSparqlBase:
		// Handle directives using TTL parser
		t.backup()
		return d.parseDirectiveTriG
	case tokenEOF:
		return nil
	case tokenIRIAbs, tokenIRIRel, tokenPrefixLabel:
		// Could be start of a graph block or a triple
		t.backup()
		return d.parseGraphOrTriple
	default:
		// Default to triple parsing
		t.backup()
		return parseTriple
	}
}

// parseDirectiveTriG handles prefix and base directives.
func (d *trigReader) parseDirectiveTriG(t *ttlReader) parseFn {
	switch t.next().typ {
	case tokenPrefix:
		label := t.expect1As("prefix label", tokenPrefixLabel)
		tok := t.expectAs("prefix RdfIRI", tokenIRIAbs, tokenIRIRel)
		if tok.typ == tokenIRIRel {
			t.ns[label.text] = t.base.String() + tok.text
		} else {
			t.ns[label.text] = tok.text
		}
		t.expect1As("directive trailing dot", tokenDot)
	case tokenSparqlPrefix:
		label := t.expect1As("prefix label", tokenPrefixLabel)
		uri := t.expect1As("prefix RdfIRI", tokenIRIAbs)
		t.ns[label.text] = uri.text
	case tokenBase:
		tok := t.expectAs("base RdfIRI", tokenIRIAbs, tokenIRIRel)
		if tok.typ == tokenIRIRel {
			t.base = IRI(t.base.String() + tok.text)
		} else {
			t.base = IRI(tok.text)
		}
		t.expect1As("directive trailing dot", tokenDot)
	case tokenSparqlBase:
		uri := t.expect1As("base RdfIRI", tokenIRIAbs)
		t.base = IRI(uri.text)
	}
	return d.parseStartTriG
}

// parseGraphOrTriple determines if we're parsing a named graph block or a regular triple.
func (d *trigReader) parseGraphOrTriple(t *ttlReader) parseFn {
	// Look ahead to see if this is a graph block: graphName { ... }
	// We need to check: IRI (or prefixed name) followed by '{'
	
	tok1 := t.next() // First token (potential graph name)
	
	switch tok1.typ {
	case tokenIRIAbs:
		// Absolute IRI as graph name
		tok2 := t.peek()
		
		if tok2.typ == tokenGraphStart {
			// This is a named graph block
			d.currentGraph = IRI(tok1.text)
			t.next() // consume '{'
			return d.parseGraphBlock
		}
		
		// Not a graph block - back up and parse as regular triple
		t.backup()
		return parseTriple
		
	case tokenIRIRel:
		// Relative IRI as graph name - resolve against base
		tok2 := t.peek()
		
		if tok2.typ == tokenGraphStart {
			// This is a named graph block
			d.currentGraph = IRI(t.base.String() + tok1.text)
			t.next() // consume '{'
			return d.parseGraphBlock
		}
		
		// Not a graph block - back up and parse as regular triple
		t.backup()
		return parseTriple
		
	case tokenPrefixLabel:
		// Prefixed name as graph name - need to resolve prefix and get suffix
		tok2 := t.peek()
		
		if tok2.typ == tokenGraphStart {
			// This is a named graph block
			ns, ok := t.ns[tok1.text]
			if !ok {
				t.errorf("missing namespace for prefix: '%s'", tok1.text)
				return nil
			}
			// Prefixed name without suffix - just use the namespace
			d.currentGraph = IRI(ns)
			t.next() // consume '{'
			return d.parseGraphBlock
		}
		
		if tok2.typ == tokenIRISuffix {
			// Check if this is graphName:suffix followed by '{'
			t.next() // consume suffix
			tok3 := t.peek()
			
			if tok3.typ == tokenGraphStart {
				// This is a named graph block with prefixed name
				ns, ok := t.ns[tok1.text]
				if !ok {
					t.errorf("missing namespace for prefix: '%s'", tok1.text)
					return nil
				}
				d.currentGraph = IRI(ns + tok2.text)
				t.next() // consume '{'
				return d.parseGraphBlock
			}
		}
		
		// Not a graph block - back up and parse as regular triple
		t.backup()
		return parseTriple
		
	default:
		// Not a graph name - back up and parse as regular triple
		t.backup()
		return parseTriple
	}
}

// parseGraphBlock parses the content inside a named graph block: { triples }
// The graph name has already been stored in d.currentGraph, and '{' has been read.
func (d *trigReader) parseGraphBlock(t *ttlReader) parseFn {
	// Parse triples (and inline prefixes) until we encounter '}'
	for {
		tok := t.peek()
		
		switch tok.typ {
		case tokenGraphEnd:
			// End of graph block
			t.next() // consume '}'
			d.currentGraph = "" // Reset current graph
			return d.parseStartTriG
			
		case tokenEOF:
			t.errorf("unexpected EOF in graph block, expected '}'")
			return nil
			
		case tokenPrefix, tokenSparqlPrefix:
			// Prefix declarations can appear inside graph blocks too
			// Handle them and continue parsing
			d.parsePrefixInGraph(t)
			
		default:
			// Parse a triple within the graph block
			state := parseTriple
			for state != nil {
				state = state(t)
			}
		}
	}
}

// parsePrefixInGraph handles prefix declarations that appear inside graph blocks.
func (d *trigReader) parsePrefixInGraph(t *ttlReader) {
	switch t.next().typ {
	case tokenPrefix:
		label := t.expect1As("prefix label", tokenPrefixLabel)
		tok := t.expectAs("prefix RdfIRI", tokenIRIAbs, tokenIRIRel)
		if tok.typ == tokenIRIRel {
			t.ns[label.text] = t.base.String() + tok.text
		} else {
			t.ns[label.text] = tok.text
		}
		t.expect1As("directive trailing dot", tokenDot)
	case tokenSparqlPrefix:
		label := t.expect1As("prefix label", tokenPrefixLabel)
		uri := t.expect1As("prefix RdfIRI", tokenIRIAbs)
		t.ns[label.text] = uri.text
	}
}

// readAll parses the entire TriG document and returns all valid triples.
// Note: This returns triples without graph context. For full quad parsing,
// the RdfRead function uses the internal currentGraph state.
func (d *trigReader) readAll() ([]rdfTriple, error) {
	var ts []rdfTriple
	for t, err := d.read(); err != io.EOF; t, err = d.read() {
		if err != nil {
			return nil, err
		}
		ts = append(ts, t)
	}
	return ts, nil
}
