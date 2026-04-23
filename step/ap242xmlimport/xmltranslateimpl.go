// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package ap242xmlimport

import (
	"encoding/xml"
	_ "unsafe" // for go:linkname
)

//go:linkname xmlTranslate encoding/xml.(*Decoder).translate
func xmlTranslate(d *xml.Decoder, n *xml.Name, isElementName bool)
