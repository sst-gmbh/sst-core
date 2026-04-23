// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package ap242xmlimport

import (
	"encoding/xml"
	"errors"
	"fmt"
	"strings"

	"github.com/jtolds/gls"
)

var (
	lsManager     = gls.NewContextManager()
	xmlDecoderKey = gls.GenSym()
)

var (
	errOutsideDecodeWithXSIType = errors.New("outside DecodeWithXSIType")
	errUnrecognizedXSIType      = errors.New("unrecognized xsi type")
)

func xmlDecodeWithXSIType(d *xml.Decoder, v interface{}) (err error) {
	lsManager.SetValues(gls.Values{xmlDecoderKey: d}, func() {
		err = d.Decode(v)
	})
	return err
}

type XSIType xml.Name

func (t *XSIType) UnmarshalXMLAttr(attr xml.Attr) error {
	d, ok := lsManager.GetValue(xmlDecoderKey)
	if !ok {
		return errOutsideDecodeWithXSIType
	}
	sl := strings.SplitN(attr.Value, ":", 2)
	switch len(sl) {
	case 2:
		*t = XSIType(xml.Name{Space: strings.TrimSpace(sl[0]), Local: strings.TrimSpace(sl[1])})
	case 1:
		*t = XSIType(xml.Name{Local: strings.TrimSpace(sl[0])})
	default:
		return fmt.Errorf("%v: %w", attr.Value, errUnrecognizedXSIType)
	}
	xmlTranslate(d.(*xml.Decoder), (*xml.Name)(t), true)
	return nil
}
