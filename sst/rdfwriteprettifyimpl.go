// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"unsafe"
)

type rdfPartialTripleWriter struct {
	f RdfFormat
	w *errWriter
}

func writeToTripleWriter(e *tripleWriter, buf []byte) {
	pe := (*rdfPartialTripleWriter)(unsafe.Pointer(e))
	pe.w.write(buf)
}

func tripleWriterFormat(e *tripleWriter) RdfFormat {
	pe := (*rdfPartialTripleWriter)(unsafe.Pointer(e))
	return pe.f
}
