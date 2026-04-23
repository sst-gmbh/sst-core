// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

/*
# SST Binary File: Overview

An SST Binary File:
  - is a kind of RDF format (similar to XML, Turtle, N3 ...) to store all triples of a single NamedGraph.
    The triples of a NamedGraph are those where the IRI of the subject node is the IRI of the NamedGraph
    or in case of a blank node that are created in that NamedGraph.
  - is used as an exchange format by the SST API and also to store the content of NamedGraphs within an SST Repository
  - is speed optimized for reading by choosing a binary format with highly normalized / canonical form;
    meaning that a given NamedGraph has only a single representation as an SST file without any variants.
  - allows extraction of the difference between two revisions of the same file very fast.
  - can also be used as a diff file format between two variants of the same namedGraph
  - is stored as a sequence of bytes as defined in this document
  - format can also be used as a stream between processes

An SST Binary File does not:
  - support any kind of comments
  - is not intended to support any specific ordering or arrangement from an original source file
  - does not support any kind of specific revision information as this is managed by the SST Repository

All lengths are stored as unsigned integers in one or several bytes. For each of these bytes only the lower 7
bits are taken (position 0 to 6), while the highest bit (position 7) is indicating if another length byte is following.
So with
  - a single byte the length 0 to 127 can be encoded (2exp7-1),
  - with 2 bytes the length 128 to 16.382 (2exp14-1)
  - with 3 bytes the length 16.384 to 2.097.151 (2exp21-1)
  - with 4 bytes the length 2.097.152 to 268.435.455 (2exp35-1)
  - with 5 bytes the length 268.435.456 to 34.359.738.368 (2exp28-1)
  - for practical reasons it is not expected to ever need a longer length than this but can be added if needed
  - encoding is **big endian** , so the first byte in the file/stream carries the higher level bits

Indexes are stored as unsigned integers in the same format as lengths (see above).

All strings are saved in Unicode UTF-8 format. Each string is prefixed with a length (see above).
There are no additional bytes for a string; e.g. there in no terminating 00 value

The sequence of bytes in an "SST Binary File" have a fixed order as follows:

# SST Binary File: Header section

This section defines all involved NamedGraphs and IBNodes.
This includes all IBNodes that are referenced as predicate or object in triples of this NamedGraph.
Each NamedGraph is given a consecutive number with this NamedGraph having the number 0. Also each IBNode
is given a consecutive number starting with 0 throughout all NamedGraphs. The byte order is as follows:
 1. a constant prefix consisting of the 8 bytes "SST-1.0" followed by the value 0 (byte).
    This is to be able to distinguish this file from others
 2. the IRI (i.e. URN-UUID for application NamedGraphs or the standard URL/URN) string of this NamedGraph.
 3. the number of explicitly **imported**  NamedGraphs
 4. loop over all imported NamedGraph, sorted alphabetically according the IRI (e.g. a URN-UUID)
    - the IRI (i.e. URN-UUID for application NamedGraphs or the standard URL/URN) string of the imported NamedGraph;
    might be an sortedReferencedGraphs IRI managed by another organization
 5. the number of other referenced NamedGraphs (that are not imported, e.g. rdf, rdfs, owl, lci, ...)
    for each other referenced NamedGraph, sorted alphabetically:
    - the IRI string of the referenced NamedGraphs

# SST Binary File: Dictionary section
 1. the number of IBNodes in **this**  NamedGraph that are not blank nodes
 2. the number of IBNodes in **this**  NamedGraph that are blank nodes, including Termcollections
 3. loop over all IBNodes  in **this**  NamedGraph (non-blank and blank)
    - the fragment strings of the IBNode in this NamedGraph that are not blank nodes, sorted alphabetically
    - the total number of triples (forward, literal and inverses).
    This information is used to fill IBNode.triplexStart and .triplexEnd and at the end of the loop namedGraph.triplexStorage.
    So the size of all arrays to be allocated is know.
 4. Note: the order of IBNode with blank nodes is sorted according their triples (see below),
    nothing stored in the file at this place.
 5. loop over all imported NamedGraph, sorted alphabetically according the IRI (e.g. a URN-UUID)
    - the number of IBNodes in the imported NamedGraph, that are directly referenced
    - loop over the referenced ST in the imported NG, sorted alphabetically according the fragment
    - > write out the fragment string,
    - > write out the number of references (= inverse hTs)
 6. for each referenced NamedGraph, sorted alphabetically:
    - the number of referenced IBNodes in the referenced NamedGraph
    - loop over all IBNodes  in one imported NamedGraph (non-blank and blank)
    - > write out the number of references (= inverse hTs).

# SST Binary File: Content section

This section contains the triples as defined in this (the main) NamedGraph. Triples defined for other NamedGraphs
are not stored in this file. The byte order is as follows:

	For each IBNode (non-blank and blank) given in the same order as in the dictionary section:
	 1. the number of triples that references another IRI resource as object (non literals)
	 2. for each triples sorted according to first predicated and second object
	  - the index of the IBNode that is used as predicated
	  - the index of the IBNode that is used as object
	  - TermCollections: TBD ?
	 3. the number of triples that contain a literal as object
	 4. for each triples sorted according to first predicated and the RDF string representation (??) of the literal
	   - the index of the IBNode that is used as predicated
	   - a single byte to decode the kind of literal
	   - an encoding of the literal, depending of the type of literal, see below
	   - Literal Collections: TBD ?

# SST Binary File: Literal encoding

See also https://www.w3.org/TR/rdf11-concepts/

  - 0 for xsd:string, followed by a string
  - 1 for rdf:langString, followed by a value string and a language string
  - 2 for xsd:boolean, followed by the byte 1 for true or 0 for false
  - 3 for xsd:decimal, followed by a string with an Arbitrary-precision decimal numbers
  - 4 for xsd:integer, followed by a string with an Arbitrary-size integer numbers
  - 5 for xsd:double for 64-bit floating point number
  - 6 for xsd:float, followed by 4 bytes for a 32-bit floating point numbers incl. ±Inf, ±0, NaN
  - 7 for xsd:date, followed by a string with a date (yyyy-mm-dd) with or without timezone
  - 8 for xsd:time, followed by a string with a times (hh:mm:ss.sss…) with or without timezone
  - 9 for xsd:dateTime, followed by a string with a date and time with or without timezone
  - 10 for xsd:dateTimeStamp, followed by a string with a date and time with required timezone
  - 11 for xsd:gYear, followed by a string with a Gregorian calendar year
  - 12 for xsd:gMonth, followed by a string with a Gregorian calendar month
  - 13 for xsd:gDay, followed by a string with a Gregorian calendar day of the month
  - 14 for xsd:gYearMonth, followed by a string with a Gregorian calendar year and month
  - 15 for xsd:gMonthDay, followed by a string with a Gregorian calendar month and day
  - 16 for xsd:duration, followed by a string with a Duration of time
  - 17 for xsd:yearMonthDuration, followed by a string with a Duration of time (months and years only)
  - 18 for xsd:dayTimeDuration, followed by a string with a Duration of time (days, hours, minutes, seconds only)
  - 19 for xsd:byte:  followed by a single byte, -128…+127 (8 bit)
  - 20 for xsd:short: followed by 2 bytes for	-32768…+32767 (16 bit)
  - 21 for xsd:int: followed by 4 bytes for	-2147483648…+2147483647 (32 bit)
  - 22 for xsd:long: followed by 8 bytes for -9223372036854775808…+9223372036854775807 (64 bit)
  - 23 for xsd:unsignedByte, followed by a single byte for 0…255 (8 bit)
  - 24 for xsd:unsignedShort, followed by 2 bytes for 0…65535 (16 bit)
  - 25 for xsd:unsignedInt, followed by 4 bytes for 0…4294967295 (32 bit)
  - 26 for xsd:unsignedLong, followed by 8 bytes for 0…18446744073709551615 (64 bit)
  - 27 for xsd:positiveInteger, followed by a string with a Integer numbers >0
  - 28 for xsd:nonNegativeInteger, followed by a string with an Integer numbers ≥0
  - 29 for xsd:negativeInteger, followed by a string with an Integer numbers <0
  - 30 for xsd:nonPositiveInteger, followed by a string with an Integer numbers ≤0
  - 31 for xsd:hexBinary, followed by a length and the length bytes for a Hex-encoded binary data
  - 32 for xsd:base64Binary, followed by a length and the length bytes Base64-encoded binary data
  - 127 for rdf:List consisting of literals only followed by the number of list members
    and for each member the literal encoded using literal encoding rules

----------

  - ? xsd:language	Language tags per [BCP47]
  - ? xsd:normalizedString	Whitespace-normalized strings
  - ? xsd:token	Tokenized strings
  - ? xsd:NMTOKEN	XML NMTOKENs
  - ? xsd:Name	XML Names
  - ? xsd:NCName	XML NCNames

# Binary Diff Segment Format

Header Section

	TBD- a constant prefix consisting of the 8 bytes "SST-Diff-1.0" followed by the value 0 (byte).

	TBD- the IRI (i.e. URN-UUID for application NamedGraphs or the standard URL/URN) string of this NamedGraph.

	TBD- the Hash value of the first NamedGraph/file to compare

	TBD- the Hash value of the second NamedGraph/file to compare

	OK- Number of removed imported NamedGraphs

	OK- Number of added imported NamedGraphs

	?- Loop over imported NamedGraph combined in one list sorted alphabetically by URI:

	?- -  Entry type byte: 0 - for a span of identical entries; 1 - removed entry; 2 - added entry

	?- -  for 0: number of identical entries (>0)

	?- -  for 1 or 2: URI of NamedGraph

	OK- Number of removed referenced NamedGraphs

	OK- Number of added referenced NamedGraphs

	?- Loop over referenced NamedGraph combined in one list sorted alphabetically by URI:

	?- -  Entry type byte: 0 - for a span of identical entries; 1 - removed entry; 2 - added entry

	?- -  for 0: number of identical entries (>0)

	?- -  for 1 or 2: URI of NamedGraph

# Dictionary section

Virtually create combined dictionary that contains all IBNodes from both NamedGraphs.

			OK- Number of IRINodes in **this**  NamedGraph that were deleted by this diff

			OK- Number of IRINodes in **this**  NamedGraph that were created by this diff

			OK- Number of BlankNodes in **this**  NamedGraph that were deleted by this diff

			OK- Number of BlankNodes in **this**  NamedGraph that were created by this diff

			OK- Loop over IRINodes in **this** NamedGraph that are combined in one list sorted alphabetically by
			  fragment:

			OK- -  Entry type byte:
		         0 - for a span of identical IBNodes;
		         1 - removed IBNode;
		         2 - added IBNode;
		         3 - same IBNode but total subject triple count differs

			OK- -  for 0: number of identical IBNodes (>0)

			OK- -  for 1 or 2: the fragment strings of the IBNode

			OK- -  for 1, 2, or 3: the subject triple count difference (>=0 for added half triples, <0 for removed half triples)

			?- Loop over IBNodes in **this**  NamedGraph that are blank nodes combined in one list sorted alphabetically by
			  canonical order:

			?- -  Entry type byte: 0 - for a span of identical IBNodes; 1 - removed IBNode; 2 - added IBNode; 3 - same IBNode but total half triple count differs

			?- -  for 0: number of identical IBNodes (>0)

			?- -  for 1, 2, or 3: the half triple count difference (>=0 for added half triples, <0 for removed half triples)

			?- loop over all combined imported NamedGraphs, sorted alphabetically according the IRI (e.g. a URN-UUID)

			?- - the number of directly referenced IBNodes in the imported NamedGraph that dropped their references by this diff

			?- - the number of directly referenced IBNodes in the imported NamedGraph that added their references by this diff

			?- -  loop over the combined list of referenced ST in the imported NG, sorted alphabetically according the fragment

			?- -  Entry type byte: 0 - for a span of identical IBNodes; 1 - removed IBNode; 2 - added IBNode; 3 - same IBNode but total half triple count differs

			?- -  for 0: number of identical IBNodes (>0)

			?- -  for 1 or 2: the fragment strings of the IBNode

			?- -  for 1, 2, or 3: the half triple count difference (>=0 for added half triples, <0 for removed half triples)

			ok- loop over all combined referenced NamedGraphs, sorted alphabetically according the IRI (e.g. a URN-UUID)

			ok- - the number of directly referenced IBNodes in the referenced NamedGraph that dropped their references by this diff

			ok- - the number of directly referenced IBNodes in the referenced NamedGraph that added their references by this diff

			ok- -  loop over the combined list of referenced ST in the referenced NG, sorted alphabetically according the fragment

			ok - -  Entry type byte:
	                          0 - for a span of identical IBNodes;
	                          1 - removed IBNode;
	                          2 - added IBNode;
	                          3 - same IBNode but total half triple count differs

			?- -  for 0: number of identical IBNodes (>0)

			?- -  for 1 or 2: the fragment strings of the IBNode

			ok- -  for 1, 2, or 3: the subject triple count difference (>=0 for added half triples, <0 for removed half triples)

# Content section
LSB - Least Signaficant Bit
For referencing IBNodes in triples as predicate or object the combined index for all IBNodes is used.

  - for each IBNode from combined index (IRI and blank Nodes) given in the same order as in the dictionary section:
    ??- -   one of following:
    ok- -   either number of IBNodes with identical triples (>0) starting from bit 1 (i.e. n<<1)
  - -   or for each changed IBNode
    ok - -   the number of removed non literal triples starting from bit 1 with LSB set to 1 (i.e. (n<<1) | 1)
    ok - -   the number of added non literal triples
  - - > for each changed IBNode non literal triple sorted according to first predicated and second object
    ??- -  one of following:
  - -  : either number of identical triples (>0) starting from bit 2 (i.e. n<<2)
  - -  : or the index of the removed IBNode that is used as predicate starting from bit 2 with 2 LSBs set to 01 (i.e. (i<<2) | 1)
  - -  : and the index of the IBNode that is used as object
    ok - -  : or the index of the added IBNode that is used as predicate starting from bit 2 with 2 LSBs set to 10 (i.e. (i<<2) | 2)
    ok - -  : and the index of the IBNode that is used as object
    ok - -   the number of removed literal triples
    ok - -   the number of added literal triples
  - - > for each literal triple sorted according to first predicated and second literal
  - -  : either number of identical triples (>0) starting from bit 2 (i.e. n<<2)
  - -  : or the index of the removed IBNode that is used as predicate starting from bit 2 with 2 LSBs set to 01 (i.e. (i<<2) | 1)
  - - > : and a single byte to decode the kind of literal
  - - > : and an encoding of the literal, depending of the type of literal, see below.
  - -  : or the index of the added IBNode that is used as predicate starting from bit 2 with 2 LSBs set to 10 (i.e. (i<<2) | 2)
  - - > : and a single byte to decode the kind of literal
  - - > : and an encoding of the literal, depending of the type of literal, see below.
*/

package sst
