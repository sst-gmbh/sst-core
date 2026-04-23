// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"fmt"
	"io"
	"runtime"
	"strconv"
	"time"
)

type ttlReader struct {
	l *lexer

	state      parseFn           // state of parser
	base       IRI               // base (default RdfIRI)
	blankNodeN int               // anonymous blank node counter
	ns         map[string]string // map[prefix]namespace
	tokens     [3]token          // 3 token lookahead
	peekCount  int               // number of tokens peeked at (position in tokens lookahead array)
	current    ctxTriple         // the current triple being parsed

	// ctxStack keeps track of current and parent triple contexts,
	// needed for parsing recursive structures (list/collections).
	ctxStack []ctxTriple

	// triples contains complete triples ready to be emitted. Usually it will have just one triple,
	// but can have more when parsing nested list/collections. read() will always return the first item.
	triples []rdfTriple
}

func newTTLReader(r io.Reader) *ttlReader {
	return &ttlReader{
		l:        newLexer(r),
		ns:       make(map[string]string),
		ctxStack: make([]ctxTriple, 0, 8),
		triples:  make([]rdfTriple, 0, 4),
	}
}

// setOption sets a ParseOption to the give value
func (d *ttlReader) setOption(o parseOption, v interface{}) error {
	switch o {
	case base:
		iri, ok := v.(IRI)
		if !ok {
			return fmt.Errorf("ParseOption \"Base\" must be an RdfIRI")
		}
		d.base = iri
	default:
		return fmt.Errorf("RDF/XML reader doesn't support option: %v", o)
	}
	return nil
}

// read parses a Turtle document, and returns the next valid triple, or an error.
func (d *ttlReader) read() (t rdfTriple, err error) {
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
	for d.state = parseStart; d.state != nil; {
		d.state = d.state(d)
	}

	if len(d.triples) == 0 {
		// No triples to emit, i.e only comments and possibly directives was parsed.
		return t, io.EOF
	}

done:
	t = d.triples[0]
	d.triples = d.triples[1:]
	return t, err
}

// readAll parses a compete Turtle document and returns the valid triples,
// or an error.
func (d *ttlReader) readAll() ([]rdfTriple, error) {
	var ts []rdfTriple
	for t, err := d.read(); err != io.EOF; t, err = d.read() {
		if err != nil {
			return nil, err
		}
		ts = append(ts, t)
	}
	return ts, nil
}

// parseStart parses top context
func parseStart(d *ttlReader) parseFn {
	tok := d.next()
	switch tok.typ {
	case tokenPrefix:
		label := d.expect1As("prefix label", tokenPrefixLabel)
		if label.text == "" {
			println("empty label")
		}
		tok := d.expectAs("prefix RdfIRI", tokenIRIAbs, tokenIRIRel)
		if tok.typ == tokenIRIRel {
			// Resolve against document base RdfIRI
			d.ns[label.text] = d.base.String() + tok.text
		} else {
			d.ns[label.text] = tok.text
		}
		d.expect1As("directive trailing dot", tokenDot)
	case tokenSparqlPrefix:
		label := d.expect1As("prefix label", tokenPrefixLabel)
		uri := d.expect1As("prefix RdfIRI", tokenIRIAbs)
		d.ns[label.text] = uri.text
	case tokenBase:
		tok := d.expectAs("base RdfIRI", tokenIRIAbs, tokenIRIRel)
		if tok.typ == tokenIRIRel {
			// Resolve against document base RdfIRI
			d.base = IRI(d.base.String() + tok.text)
		} else {
			d.base = IRI(tok.text)
		}
		d.expect1As("directive trailing dot", tokenDot)
	case tokenSparqlBase:
		uri := d.expect1As("base RdfIRI", tokenIRIAbs)
		d.base = IRI(uri.text)
	case tokenGraphStart, tokenGraphEnd:
		d.errorf("unexpected character: '%s'", tok.text)
		return nil
	case tokenEOF:
		return nil
	default:
		d.backup()
		return parseTriple
	}
	return parseStart
}

// parseEnd parses punctuation [.,;\])] before emitting the current triple.
func parseEnd(d *ttlReader) parseFn {
	tok := d.next()
	switch tok.typ {
	case tokenSemicolon:
		switch d.peek().typ {
		case tokenSemicolon:
			// parse multiple semicolons in a row
			return parseEnd
		case tokenDot:
			// parse trailing semicolon
			return parseEnd
		case tokenPropertyListEnd:
			// parse trailing semicolon before property list end (e.g., "[ p o ; ]")
			return parseEnd
		case tokenEOF:
			// trailing semicolon without final dot not allowed
			// TODO only allowed in property lists?
			d.errorf("%d:%d: expected triple termination, got %v", tok.line, tok.col, tok.typ)
			return nil
		}
		d.current.Pred = nil
		d.current.Obj = nil
		d.pushContext()
		return nil
	case tokenComma:
		d.current.Obj = nil
		d.pushContext()
		return nil
	case tokenPropertyListEnd:
		d.popContext()
		if d.peek().typ == tokenDot {
			// Reached end of statement
			d.next()
			return nil
		}
		if d.current.Pred == nil {
			// Property list was subject, push context with subject to stack.
			d.pushContext()
			return nil
		}
		// Property list was object, need to check for more closing property lists.
		return parseEnd
	case tokenCollectionEnd:
		// Emit collection closing triple { blankNodeN rdf:rest rdf:nil }
		d.current.Pred = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#rest")
		d.current.Obj = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil")
		d.emit()

		// Restore parent triple
		d.popContext()
		if d.current.Pred == nil {
			// Collection was subject, push context with subject to stack.
			d.pushContext()
			return nil
		}
		// Collection was object, need to check for more closing collection.
		return parseEnd
	case tokenDot:
		if d.current.Ctx == ctxColl {
			return parseEnd
		}
		return nil
	case tokenError:
		d.errorf("%d:%d: syntax error: %v", tok.line, tok.col, tok.text)
		return nil
	default:
		if d.current.Ctx == ctxColl {
			d.backup() // unread collection item, to be parsed on next iteration

			d.blankNodeN++
			d.current.Pred = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#rest")
			d.current.Obj = rdfTypeBlank{id: fmt.Sprintf("_:b%d", d.blankNodeN)}
			d.emit()

			d.current.Subj = d.current.Obj.(rdfTypeSubject)
			d.current.Obj = nil
			d.current.Pred = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#first")
			d.pushContext()
			return nil
		}
		d.errorf("%d:%d: expected triple termination, got %v", tok.line, tok.col, tok.typ)
		return nil
	}
}

func parseTriple(d *ttlReader) parseFn {
	return parseSubject
}

func parseSubject(d *ttlReader) parseFn {
	// restore triple context, or clear current
	d.popContext()

	if d.current.Subj != nil {
		return parsePredicate
	}
	tok := d.next()
	switch tok.typ {
	case tokenIRIAbs:
		d.current.Subj = IRI(tok.text)
	case tokenIRIRel:
		d.current.Subj = IRI(d.base.String() + tok.text)
	case tokenBNode:
		d.current.Subj = rdfTypeBlank{id: tok.text}
	case tokenAnonBNode:
		d.blankNodeN++
		d.current.Subj = rdfTypeBlank{id: fmt.Sprintf("_:b%d", d.blankNodeN)}
	case tokenPrefixLabel:
		ns, ok := d.ns[tok.text]
		if !ok {
			d.errorf("missing namespace for prefix: '%s'", tok.text)
		}
		suf := d.expect1As("RdfIRI suffix", tokenIRISuffix)
		d.current.Subj = IRI(ns + suf.text)
	case tokenPropertyListStart:
		// Blank node is subject of a new triple
		d.blankNodeN++
		d.current.Subj = rdfTypeBlank{id: fmt.Sprintf("_:b%d", d.blankNodeN)}
		d.pushContext() // Subj = blankNodeN, top context
		d.current.Ctx = ctxList
	case tokenCollectionStart:
		if d.peek().typ == tokenCollectionEnd {
			// An empty collection
			d.current.Subj = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil")
			break
		}
		d.blankNodeN++
		d.current.Subj = rdfTypeBlank{id: fmt.Sprintf("_:b%d", d.blankNodeN)}
		d.pushContext()
		d.current.Pred = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#first")
		d.current.Ctx = ctxColl
		return parseObject
	case tokenError:
		d.errorf("%d:%d: syntax error: %v", tok.line, tok.col, tok.text)
	default:
		d.errorf("unexpected %v as subject", tok.typ)
	}

	return parsePredicate
}

func parsePredicate(d *ttlReader) parseFn {
	if d.current.Pred != nil {
		return parseObject
	}
	tok := d.next()
	switch tok.typ {
	case tokenIRIAbs:
		d.current.Pred = IRI(tok.text)
	case tokenIRIRel:
		d.current.Pred = IRI(d.base.String() + tok.text)
	case tokenRDFType:
		d.current.Pred = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
	case tokenPrefixLabel:
		ns, ok := d.ns[tok.text]
		if !ok {
			d.errorf("missing namespace for prefix: '%s'", tok.text)
		}
		suf := d.expect1As("RdfIRI suffix", tokenIRISuffix)
		d.current.Pred = IRI(ns + suf.text)
	case tokenError:
		d.errorf("%d:%d: syntax error: %v", tok.line, tok.col, tok.text)
	default:
		d.errorf("%d:%d: unexpected %v as predicate", tok.line, tok.col, tok.typ)
	}

	return parseObject
}

func parseObject(d *ttlReader) parseFn {
	tok := d.next()
	switch tok.typ {
	case tokenIRIAbs:
		d.current.Obj = IRI(tok.text)
	case tokenIRIRel:
		d.current.Obj = IRI(d.base.String() + tok.text)
	case tokenBNode:
		d.current.Obj = rdfTypeBlank{id: tok.text}
	case tokenAnonBNode:
		d.blankNodeN++
		d.current.Obj = rdfTypeBlank{id: fmt.Sprintf("_:b%d", d.blankNodeN)}
	case tokenLiteral, tokenLiteral3:
		val := tok.text
		l := rdfTypeLiteral{
			str:      val,
			DataType: xsdString,
		}
		p := d.peek()
		switch p.typ {
		case tokenLangMarker:
			d.next() // consume peeked token
			tok = d.expect1As("literal language", tokenLang)
			l.lang = tok.text
			l.DataType = rdfLangString
		case tokenDataTypeMarker:
			d.next() // consume peeked token
			tok = d.expectAs("literal datatype", tokenIRIAbs, tokenPrefixLabel)
			switch tok.typ {
			case tokenIRIAbs:
				l.DataType = IRI(tok.text)
			case tokenPrefixLabel:
				ns, ok := d.ns[tok.text]
				if !ok {
					d.errorf("missing namespace for prefix: '%s'", tok.text)
				}
				tok2 := d.expect1As("RdfIRI suffix", tokenIRISuffix)
				l.DataType = IRI(ns + tok2.text)
			}
		}
		d.current.Obj = l
	case tokenLiteralDouble:
		d.current.Obj = rdfTypeLiteral{
			str:      tok.text,
			DataType: xsdDouble,
		}
	case tokenLiteralDecimal:
		d.current.Obj = rdfTypeLiteral{
			str:      tok.text,
			DataType: xsdDecimal,
		}
	case tokenLiteralInteger:
		d.current.Obj = rdfTypeLiteral{
			str:      tok.text,
			DataType: xsdInteger,
		}
	case tokenLiteralBoolean:
		d.current.Obj = rdfTypeLiteral{
			str:      tok.text,
			DataType: xsdBoolean,
		}
	case tokenPrefixLabel:
		ns, ok := d.ns[tok.text]
		if !ok {
			d.errorf("missing namespace for prefix: '%s'", tok.text)
		}
		suf := d.expect1As("RdfIRI suffix", tokenIRISuffix)
		d.current.Obj = IRI(ns + suf.text)
	case tokenPropertyListStart:
		// Blank node is object of current triple
		// Save current context, to be restored after the list ends
		d.pushContext()

		d.blankNodeN++
		d.current.Obj = rdfTypeBlank{id: fmt.Sprintf("_:b%d", d.blankNodeN)}
		d.emit()

		// Set blank node as subject of the next triple. Push to stack and return.
		d.current.Subj = d.current.Obj.(rdfTypeSubject)
		d.current.Pred = nil
		d.current.Obj = nil
		d.current.Ctx = ctxList
		d.pushContext()
		return nil
	case tokenCollectionStart:
		if d.peek().typ == tokenCollectionEnd {
			// an empty collection
			d.next() // consume ')'
			d.current.Obj = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil")
			break
		}
		// Blank node is object of current triple
		// Save current context, to be restored after the collection ends
		d.pushContext()

		d.blankNodeN++
		d.current.Obj = rdfTypeBlank{id: fmt.Sprintf("_:b%d", d.blankNodeN)}
		d.emit()
		d.current.Subj = d.current.Obj.(rdfTypeSubject)
		d.current.Pred = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#first")
		d.current.Obj = nil
		d.current.Ctx = ctxColl
		d.pushContext()
		return nil
	case tokenError:
		d.errorf("%d:%d: syntax error: %v", tok.line, tok.col, tok.text)
	default:
		d.errorf("%d:%d: unexpected %v as object", tok.line, tok.col, tok.typ)
	}

	// We now have a full tripe, emit it.
	d.emit()

	return parseEnd
}

// pushContext pushes the current triple and context to the context stack.
func (d *ttlReader) pushContext() {
	d.ctxStack = append(d.ctxStack, d.current)
}

// popContext restores the next context on the stack as the current context.
// If already at the topmost context, it clears the current triple.
func (d *ttlReader) popContext() {
	switch len(d.ctxStack) {
	case 0:
		d.current.Ctx = ctxTop
		d.current.Subj = nil
		d.current.Pred = nil
		d.current.Obj = nil
	case 1:
		d.current = d.ctxStack[0]
		d.ctxStack = d.ctxStack[:0]
	default:
		d.current = d.ctxStack[len(d.ctxStack)-1]
		d.ctxStack = d.ctxStack[:len(d.ctxStack)-1]
	}
}

// emit adds the current triple to the slice of completed triples.
func (d *ttlReader) emit() {
	d.triples = append(d.triples, d.current.rdfTriple)
}

// next returns the next token.
func (d *ttlReader) next() token {
	if d.peekCount > 0 {
		d.peekCount--
	} else {
		d.tokens[0] = d.l.nextToken()
	}

	return d.tokens[d.peekCount]
}

// peek returns but does not consume the next token.
func (d *ttlReader) peek() token {
	if d.peekCount > 0 {
		return d.tokens[d.peekCount-1]
	}
	d.peekCount = 1
	d.tokens[0] = d.l.nextToken()
	return d.tokens[0]
}

// backup backs the input stream up one token.
func (d *ttlReader) backup() {
	d.peekCount++
}

// backup2 backs the input stream up two tokens.
func (d *ttlReader) backup2(t1 token) {
	d.tokens[1] = t1
	d.peekCount = 2
}

// backup3 backs the input stream up three tokens.
func (d *ttlReader) backup3(t2, t1 token) {
	d.tokens[1] = t1
	d.tokens[2] = t2
	d.peekCount = 3
}

// Parsing:

// parseFn represents the state of the parser as a function that returns the next state.
type parseFn func(*ttlReader) parseFn

// errorf formats the error and terminates parsing.
func (d *ttlReader) errorf(format string, args ...interface{}) {
	format = fmt.Sprintf("%s", format)
	panic(fmt.Errorf(format, args...))
}

// unexpected complains about the given token and terminates parsing.
func (d *ttlReader) unexpected(t token, context string) {
	d.errorf("%d:%d unexpected %v as %s", t.line, t.col, t.typ, context)
}

// recover catches non-runtime panics and binds the panic error
// to the given error pointer.
func (d *ttlReader) recover(err *error) {
	e := recover()
	if e != nil {
		if _, ok := e.(runtime.Error); ok {
			// Don't recover from runtime errors.
			panic(e)
		}
		// d.stop() something to clean up?
		*err = e.(error)
	}
	return
}

// expect1As consumes the next token and guarantees that it has the expected type.
func (d *ttlReader) expect1As(context string, expected tokenType) token {
	t := d.next()
	if t.typ != expected {
		if t.typ == tokenError {
			d.errorf("%d:%d: syntax error: %s", t.line, t.col, t.text)
		} else {
			d.unexpected(t, context)
		}
	}
	return t
}

// expectAs consumes the next token and guarantees that it has the one of the expected types.
func (d *ttlReader) expectAs(context string, expected ...tokenType) token {
	t := d.next()
	for _, e := range expected {
		if t.typ == e {
			return t
		}
	}
	if t.typ == tokenError {
		d.errorf("syntax error: %s", t.text)
	} else {
		d.unexpected(t, context)
	}
	return t
}

// parseLiteral
func parseLiteral(val, datatype string) (interface{}, error) {
	switch datatype {
	case xsdString.String():
		return val, nil
	case xsdInteger.String():
		i, err := strconv.Atoi(val)
		if err != nil {
			return nil, err
		}
		return i, nil
	case xsdFloat.String(), xsdDouble.String(), xsdDecimal.String():
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, err
		}
		return f, nil
	case xsdBoolean.String():
		bo, err := strconv.ParseBool(val)
		if err != nil {
			return nil, err
		}
		return bo, nil
	case xsdDateTime.String():
		t, err := time.Parse(rdfDateFormat, val)
		if err != nil {
			// Unfortunately, xsd:dateTime allows dates without timezone information
			// Try parse again unspecified timezone (defaulting to UTC)
			t, err = time.Parse("2006-01-02T15:04:05", val)
			if err != nil {
				return nil, err
			}
			return t, nil
		}
		return t, nil
	case xsdByte.String():
		return []byte(val), nil
		// TODO: other xsd datatype that maps to Go data types
	default:
		return val, nil
	}
}

// ctxTriple contains a Triple, plus the context in which the Triple appears.
type ctxTriple struct {
	rdfTriple
	Ctx contextInt
}

type contextInt int

const (
	ctxTop contextInt = iota
	ctxColl
	ctxList
)

// TODO remove when done
func (ctx contextInt) String() string {
	switch ctx {
	case ctxTop:
		return "top context"
	case ctxList:
		return "list"
	case ctxColl:
		return "collection"

	default:
		return "unknown context"
	}
}
