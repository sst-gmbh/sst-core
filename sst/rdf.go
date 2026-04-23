// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

// This source file provides functionality for working with RDF resources, including
// parsing and serialization of the various RDF formats.
//
// # Data model
//
// The package adhere to the RDF data model as described in http://www.w3.org/TR/rdf11-concepts/.
//
// # Data structures
//
// TODO.
//
// # Writing and reading
//
// The package currently supports the following RDF serialization formats:
//
//	Format     |  Read  | Write
//	-----------|--------|--------
//	Turtle     | x      | x
//	TriG       | x      | x
//
// The parsers are implemented as streaming readers, consuming an io.Reader
// and emitting triples/quads as soon as they are available. Simply call
// read() until the reader is exhausted and emits io.EOF:
//
//	f, err := os.Open("mytriples.ttl")
//	if err != nil {
//	    // handle error
//	}
//	rdr := newTripleReader(f, RdfFormatTurtle)
//	for triple, err := rdr.read(); err != io.EOF; triple, err = rdr.read() {
//	    // do something with triple ..
//	}
//
// The writers work similarly.

package sst

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	irilib "github.com/google/xtoproto/rdf/iri"
	urnlib "github.com/leodido/go-urn"
)

// rdfDateFormat defines the string representation of xsd:DateTime values. You can override
// it if you need another layout.
var rdfDateFormat = time.RFC3339

// The XML schema built-in dataTypes (xsd):
// https://dvcs.w3.org/hg/rdf/raw-file/default/rdf-concepts/index.html#xsd-datatypes
var (
	// Core types:                                                    // Corresponding Go datatype:

	xsdString  = IRI("http://www.w3.org/2001/XMLSchema#string") // string
	xsdBoolean = IRI("http://www.w3.org/2001/XMLSchema#boolean")
	xsdDecimal = IRI("http://www.w3.org/2001/XMLSchema#decimal") // float64
	xsdInteger = IRI("http://www.w3.org/2001/XMLSchema#integer") // int
	xsdShort   = IRI("http://www.w3.org/2001/XMLSchema#short")   // int16

	// IEEE floating-point numbers:

	xsdDouble = IRI("http://www.w3.org/2001/XMLSchema#double") // float64
	xsdFloat  = IRI("http://www.w3.org/2001/XMLSchema#float")  // float64

	// Time and date:

	// xsdDate = IRI("http://www.w3.org/2001/XMLSchema#date")
	// xsdTime          = IRI("http://www.w3.org/2001/XMLSchema#time")
	xsdDateTime = IRI("http://www.w3.org/2001/XMLSchema#dateTime") // time.Time
	// xsdDateTimeStamp = IRI("http://www.w3.org/2001/XMLSchema#dateTimeStamp")

	// Recurring and partial dates:

	// xsdYear              = IRI("http://www.w3.org/2001/XMLSchema#gYear")
	// xsdMonth             = IRI("http://www.w3.org/2001/XMLSchema#gMonth")
	// xsdDay               = IRI("http://www.w3.org/2001/XMLSchema#gDay")
	// xsdYearMonth         = IRI("http://www.w3.org/2001/XMLSchema#gYearMonth")
	// xsdDuration          = IRI("http://www.w3.org/2001/XMLSchema#Duration")
	// xsdYearMonthDuration = IRI("http://www.w3.org/2001/XMLSchema#yearMonthDuration")
	// xsdDayTimeDuration   = IRI("http://www.w3.org/2001/XMLSchema#dayTimeDuration")

	// Limited-range integer numbers

	xsdByte = IRI("http://www.w3.org/2001/XMLSchema#byte") // []byte
	// xsdShort = IRI("http://www.w3.org/2001/XMLSchema#short") // int16
	xsdInt = IRI("http://www.w3.org/2001/XMLSchema#int") // int32
	// xsdLong  = IRI("http://www.w3.org/2001/XMLSchema#long")  // int64

	// Various

	rdfLangString = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString") // string
	xmlLiteral    = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral") // string
)

// RdfFormat represents a RDF serialization format.
type RdfFormat int

// Supported parser/serialization formats for Triples and Quads.
const (
	// Triple serialization:

	// RdfFormatNTriples RdfFormat = iota
	// RdfFormatTurtle

	RdfFormatTurtle RdfFormat = iota
	RdfFormatTriG

	// Internal formats
	formatInternal
)

// rdfTerm represents an RDF term. There are 3 term types: Blank node, RdfTypeLiteral and RdfIRI.
type rdfTerm interface {
	// rdfSerialize returns a string representation of the Term in the specified serialization format.
	rdfSerialize(RdfFormat) string

	// String returns the term as it is stored, without any modifications.
	String() string

	// rdfTermType returns the Term type.
	rdfTermType() rdfTermType
}

// rdfTermType describes the type of RDF term: Blank node, RdfIRI or RdfTypeLiteral
type rdfTermType int

// Exported RDF term types.
const (
	rdfTermBlank rdfTermType = iota
	rdfTermIRI
	rdfTermLiteral
)

// rdfTypeBlank represents a RDF blank node; an unqualified RdfIRI with identified by a label.
type rdfTypeBlank struct {
	id string
}

// validAsSubject denotes that a Blank node is valid as a Triple's Subject.
func (b rdfTypeBlank) validAsSubject() {}

// validAsObject denotes that a Blank node is valid as a Triple's Object.
func (b rdfTypeBlank) validAsObject() {}

// rdfSerialize returns a string representation of a Blank node.
func (b rdfTypeBlank) rdfSerialize(f RdfFormat) string {
	return b.id
}

// rdfTermType returns the TermType of a blank node.
func (b rdfTypeBlank) rdfTermType() rdfTermType {
	return rdfTermBlank
}

// String returns the Blank node label
func (b rdfTypeBlank) String() string {
	return b.id[2:]
}

// rdfNewBlank returns a new blank node with a given label. It returns
// an error only if the supplied label is blank.
func rdfNewBlank(id string) (rdfTypeBlank, error) {
	if len(strings.TrimSpace(id)) == 0 {
		return rdfTypeBlank{}, errors.New("blank id")
	}
	return rdfTypeBlank{id: "_:" + id}, nil
}

// IRI represents a RDF IRI resource.
// type IRI string

// validAsSubject denotes that an RdfIRI is valid as a Triple's Subject.
func (u IRI) validAsSubject() {}

// validAsPredicate denotes that an RdfIRI is valid as a Triple's Predicate.
func (u IRI) validAsPredicate() {}

// validAsObject denotes that an RdfIRI is valid as a Triple's Object.
func (u IRI) validAsObject() {}

// rdfTermType returns the TermType of a RdfIRI.
func (u IRI) rdfTermType() rdfTermType {
	return rdfTermIRI
}

// String returns the IRI string.
func (u IRI) String() string {
	return string(u)
}

// rdfSerialize serializes the IRI into a string format based on the specified RDF format.
// It returns the IRI enclosed in angle brackets.
//
// Parameters:
//
//	f (RdfFormat): The RDF format to use for serialization.
//
// Returns:
//
//	string: The serialized IRI as a string.
func (u IRI) rdfSerialize(f RdfFormat) string {
	return fmt.Sprintf("<%s>", u)
}

// Split splits the IRI into a base and fragment based on the last occurrence of the '#' character.
// If the '#' character is not found, the entire IRI is returned as the base, and the fragment is empty.
// If the '#' character is found, the base is the part before the '#', and the fragment is the part after the '#'.
func (u IRI) Split() (base, fragment string) {
	i := len(u)
	for i > 0 {
		r, w := utf8.DecodeLastRuneInString(u.String()[0:i])
		if r == '#' {
			base, fragment = u.String()[0:i], u.String()[i:len(u)]
			break
		}
		i -= w
	}
	// if there is no "#" in the IRI, base should be IRI itself
	if len(base) == 0 && len(fragment) == 0 {
		base = u.String()
	} else {
		// else there is a "#" in IRI, there will be a "#" at the end, so trim it
		base = strings.TrimRight(base, "#")
	}
	return base, fragment
}

var nonHierarchicalSchemes = map[string]bool{
	"mailto": true,
	"tel":    true,
	"fax":    true,
	"news":   true,
}

// NewIRI returns a new IRI, or an error if it's not valid.
//
// A valid IRI cannot be empty, or contain any of the disallowed characters: [\x00-\x20<>"{}|^`\].
func NewIRI(s string) (IRI, error) {
	in := strings.TrimSpace(s)
	if in == "" {
		return "", fmt.Errorf("empty identifier")
	}

	// scheme
	i := strings.IndexByte(in, ':')
	if i <= 0 {
		return "", fmt.Errorf("missing or invalid scheme (no ':'): %q", in)
	}
	scheme := strings.ToLower(in[:i])

	// URN go RFC 8141
	if scheme == "urn" {
		// fragment：urn:<nid>:<nss>#frag
		body := in
		if j := strings.IndexByte(in, '#'); j >= 0 {
			body = in[:j] // go-urn before fragment
			frag := in[j+1:]
			if strings.ContainsAny(frag, " \t\r\n") {
				return "", fmt.Errorf("invalid fragment (whitespace): %q", frag)
			}
		}
		if _, err := urnlib.Parse([]byte(body)); !err {
			return "", fmt.Errorf("invalid URN: %s", s)
		}
		return IRI(in), nil
	}

	if nonHierarchicalSchemes[scheme] {
		opaque := in[i+1:]
		if opaque == "" {
			return "", fmt.Errorf("opaque part cannot be empty for scheme: %s", scheme)
		}
		if strings.ContainsAny(opaque, " \t\r\n") {
			return "", fmt.Errorf("opaque part contains whitespace: %q", opaque)
		}
		return IRI(in), nil

	}

	// RFC 3987 IRI
	if _, err := irilib.Parse(in); err != nil {
		return "", fmt.Errorf("invalid IRI: %w", err)
	}
	return IRI(in), nil
}

// rdfTypeLiteral represents a RDF literal; a value with a datatype and
// (optionally) an associated language tag for strings.
type rdfTypeLiteral struct {
	// The literal is always stored as a string, regardless of datatype.
	str string

	// Val represents the typed value of a RDF RdfTypeLiteral, boxed in an empty interface.
	// A type assertion is needed to get the value in the corresponding Go type.
	val interface{}

	// lang, if not empty, represents the language tag of a string.
	// A language tagged string has the datatype: rdf:langString.
	lang string

	// The datatype of the RdfTypeLiteral.
	DataType IRI
}

// rdfSerialize returns a string representation of a RdfTypeLiteral.
func (l rdfTypeLiteral) rdfSerialize(f RdfFormat) string {
	if rdfTermsEqual(l.DataType, rdfLangString) {
		return fmt.Sprintf("\"%s\"@%s", escapeLiteral(l.str), l.Lang())
	}
	if l.DataType != xsdString {
		switch f {
		case formatInternal:
			return l.str
		case RdfFormatTurtle, RdfFormatTriG:
			switch l.DataType {
			case xsdInteger, xsdDecimal, xsdBoolean, xsdDouble:
				return l.str
			case xsdDateTime:
				return fmt.Sprintf("\"%s\"^^%s", l.str, l.DataType.rdfSerialize(f))
			default:
				return fmt.Sprintf("\"%s\"^^%s", escapeLiteral(l.str), l.DataType.rdfSerialize(f))
			}
		default:
			panic("TODO")
		}
	}
	return fmt.Sprintf("\"%s\"", escapeLiteral(l.str))
}

// rdfTermType returns the TermType of a RdfTypeLiteral.
func (l rdfTypeLiteral) rdfTermType() rdfTermType {
	return rdfTermLiteral
}

// Lang returns the language of a language-tagged string.
func (l rdfTypeLiteral) Lang() string {
	return l.lang
}

// String returns the literal string.
func (l rdfTypeLiteral) String() string {
	return l.str
}

// Typed tries to parse the RdfTypeLiteral's value into a Go type, according to the
// the DataType.
func (l rdfTypeLiteral) Typed() (interface{}, error) {
	if l.val == nil {
		switch l.DataType {
		case xsdInteger, xsdInt:
			i, err := strconv.Atoi(l.str)
			if err != nil {
				return nil, err
			}
			l.val = i
			return i, nil
		case xsdDouble, xsdDecimal:
			f, err := strconv.ParseFloat(l.str, 64)
			if err != nil {
				return nil, err
			}
			l.val = f
			return f, nil
		case xsdBoolean:
			b, err := strconv.ParseBool(l.str)
			if err != nil {
				return nil, err
			}
			l.val = b
			return b, nil
		case xsdShort:
			i, err := strconv.ParseInt(l.str, 10, 16)
			if err != nil {
				return nil, err
			}
			l.val = int16(i)
			return int16(i), nil
		case xsdByte:
			return []byte(l.str), nil
			// TODO xsdDateTime etc
		default:
			return l.str, nil
		}
	}
	return l.val, nil
}

// validAsObject denotes that a RdfTypeLiteral is valid as a Triple's Object.
func (l rdfTypeLiteral) validAsObject() {}

// newLiteral returns a new RdfTypeLiteral, or an error on invalid input. It tries
// to map the given Go values to a corresponding xsd datatype.
func newLiteral(v interface{}) (rdfTypeLiteral, error) {
	switch t := v.(type) {
	case bool:
		return rdfTypeLiteral{val: t, str: fmt.Sprintf("%v", t), DataType: xsdBoolean}, nil
	case int, int32, int64:
		return rdfTypeLiteral{val: t, str: fmt.Sprintf("%v", t), DataType: xsdInteger}, nil
	case string:
		return rdfTypeLiteral{str: t, DataType: xsdString}, nil
	case float32, float64:
		return rdfTypeLiteral{val: t, str: fmt.Sprintf("%v", t), DataType: xsdDouble}, nil
	case time.Time:
		return rdfTypeLiteral{val: t, str: t.Format(rdfDateFormat), DataType: xsdDateTime}, nil
	case []byte:
		return rdfTypeLiteral{val: t, str: string(t), DataType: xsdByte}, nil
	default:
		return rdfTypeLiteral{}, fmt.Errorf("cannot infer XSD datatype from %#v", t)
	}
}

// rdfNewLangLiteral creates a RDF literal with a given language tag, or fails
// if the language tag is not well-formed.
//
// The literal will have the datatype RdfIRI xsd:String.
func rdfNewLangLiteral(v, lang string) (rdfTypeLiteral, error) {
	afterDash := false
	if len(lang) >= 1 && lang[0] == '-' {
		return rdfTypeLiteral{}, errors.New("invalid language tag: must start with a letter")
	}
	for _, r := range lang {
		switch {
		case (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z'):
			continue
		case r == '-':
			if afterDash {
				return rdfTypeLiteral{}, errors.New("invalid language tag: only one '-' allowed")
			}
			afterDash = true
		case r >= '0' && r <= '9':
			if afterDash {
				continue
			}
			fallthrough
		default:
			return rdfTypeLiteral{}, fmt.Errorf("invalid language tag: unexpected character: %q", r)
		}
	}
	if lang[len(lang)-1] == '-' {
		return rdfTypeLiteral{}, errors.New("invalid language tag: trailing '-' disallowed")
	}
	return rdfTypeLiteral{str: v, lang: lang, DataType: rdfLangString}, nil
}

// rdfNewTypedLiteral returns a literal with the given datatype.
func rdfNewTypedLiteral(v string, dt IRI) rdfTypeLiteral {
	return rdfTypeLiteral{str: v, DataType: dt}
}

// rdfTypeSubject interface distinguishes which Terms are valid as a rdfTypeSubject of a Triple.
type rdfTypeSubject interface {
	rdfTerm
	validAsSubject()
}

// rdfTypePredicate interface distinguishes which Terms are valid as a rdfTypePredicate of a Triple.
type rdfTypePredicate interface {
	rdfTerm
	validAsPredicate()
}

// Object interface distinguishes which Terms are valid as a Object of a Triple.
type rdfTypeObject interface {
	rdfTerm
	validAsObject()
}

// rdfContext interface distinguishes which Terms are valid as a Quad's rdfContext.
// Incidentally, this is the same as Terms valid as a Subject of a Triple.
type rdfContext interface {
	rdfTerm
	validAsSubject()
}

// rdfTriple represents a RDF triple.
type rdfTriple struct {
	Subj rdfTypeSubject
	Pred rdfTypePredicate
	Obj  rdfTypeObject
}

// Serialize returns a string representation of a Triple in the specified format.
//
// However, it will only serialize the triple itself, and not include the prefix directives.
// For a full serialization including directives, use the TripleWriter.
func (t rdfTriple) Serialize(f RdfFormat) string {
	var s, o string
	switch term := t.Subj.(type) {
	case IRI:
		s = term.rdfSerialize(f)
	case rdfTypeBlank:
		s = term.rdfSerialize(f)
	}
	switch term := t.Obj.(type) {
	case IRI:
		o = term.rdfSerialize(f)
	case rdfTypeLiteral:
		o = term.rdfSerialize(f)
	case rdfTypeBlank:
		o = term.rdfSerialize(f)
	}
	return fmt.Sprintf(
		"%s %s %s .\n",
		s,
		t.Pred.(IRI).rdfSerialize(f),
		o,
	)
}

// rdfQuad represents a RDF rdfQuad; a Triple plus the context in which it occurs.
type rdfQuad struct {
	rdfTriple
	Ctx rdfContext
}

// Serialize serializes the Quad in the given format (assumed to be NQuads atm).
func (q rdfQuad) Serialize(f RdfFormat) string {
	var s, o, g string
	switch term := q.Subj.(type) {
	case IRI:
		s = term.rdfSerialize(f)
	case rdfTypeBlank:
		s = term.rdfSerialize(f)
	}
	switch term := q.Obj.(type) {
	case IRI:
		o = term.rdfSerialize(f)
	case rdfTypeLiteral:
		o = term.rdfSerialize(f)
	case rdfTypeBlank:
		o = term.rdfSerialize(f)
	}
	switch term := q.Ctx.(type) {
	case IRI:
		g = term.rdfSerialize(f)
	case rdfTypeBlank:
		g = term.rdfSerialize(f)
	}
	return fmt.Sprintf(
		"%s %s %s %s .\n",
		s,
		q.Pred.(IRI).rdfSerialize(f),
		o,
		g,
	)
}

// rdfTermsEqual returns true if two Terms are equal, or false if they are not.
func rdfTermsEqual(a, b rdfTerm) bool {
	if a.rdfTermType() != b.rdfTermType() {
		return false
	}
	return a.rdfSerialize(formatInternal) == b.rdfSerialize(formatInternal)
}

// rdfTriplesEqual tests if two Triples are identical.
func rdfTriplesEqual(a, b rdfTriple) bool {
	return rdfTermsEqual(a.Subj, b.Subj) && rdfTermsEqual(a.Pred, b.Pred) && rdfTermsEqual(a.Obj, b.Obj)
}

// rdfQuadsEqual tests if two Quads are identical.
func rdfQuadsEqual(a, b rdfQuad) bool {
	return rdfTermsEqual(a.Ctx, b.Ctx) && rdfTriplesEqual(a.rdfTriple, b.rdfTriple)
}
