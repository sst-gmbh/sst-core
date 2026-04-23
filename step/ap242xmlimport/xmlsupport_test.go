// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package ap242xmlimport

import (
	"encoding/xml"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestElement struct {
	XMLName xml.Name      `xml:"element"`
	XSIType XSIType       `xml:"http://www.w3.org/2001/XMLSchema-instance type,attr"`
	Element []TestElement `xml:"element"`
}

func TestXSIType_UnmarshalXMLAttr(t *testing.T) {
	type args struct {
		inXML string
	}
	tests := []struct {
		name      string
		args      args
		assertion assert.ErrorAssertionFunc
		want      TestElement
	}{
		{
			name: "on_root_element",
			args: args{
				inXML: `<element
							xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
							xmlns:t="http://example.com/type" xsi:type="t:ExampleType">
						</element>`,
			},
			assertion: assert.NoError,
			want: TestElement{
				XMLName: xml.Name{Local: "element"},
				XSIType: XSIType{Space: "http://example.com/type", Local: "ExampleType"},
				Element: nil,
			},
		},
		{
			name: "on_root_default_ds_element",
			args: args{
				inXML: `<element
							xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
							xmlns="http://example.com/type" xsi:type="ExampleType">
						</element>`,
			},
			assertion: assert.NoError,
			want: TestElement{
				XMLName: xml.Name{Space: "http://example.com/type", Local: "element"},
				XSIType: XSIType{Space: "http://example.com/type", Local: "ExampleType"},
				Element: nil,
			},
		},
		{
			name: "on_nested_element",
			args: args{
				inXML: `<element>
							<element
								xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
								xmlns:t="http://example.com/type" xsi:type="t:ExampleType" />
						</element>`,
			},
			assertion: assert.NoError,
			want: TestElement{
				XMLName: xml.Name{Local: "element"},
				XSIType: XSIType{},
				Element: []TestElement{{
					XMLName: xml.Name{Local: "element"},
					XSIType: XSIType{Space: "http://example.com/type", Local: "ExampleType"},
				}},
			},
		},
		{
			name: "on_nested_inherited_element",
			args: args{
				inXML: `<element
							xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
							xmlns:t="http://example.com/type">
							<element xsi:type="t:ExampleType" />
						</element>`,
			},
			assertion: assert.NoError,
			want: TestElement{
				XMLName: xml.Name{Local: "element"},
				XSIType: XSIType{},
				Element: []TestElement{{
					XMLName: xml.Name{Local: "element"},
					XSIType: XSIType{Space: "http://example.com/type", Local: "ExampleType"},
				}},
			},
		},
		{
			name: "on_nested_overridden_element",
			args: args{
				inXML: `<element
							xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
							xmlns:t="http://example.com/type">
							<element xmlns:t="http://example.com/type2" xsi:type="t:ExampleType" />
						</element>`,
			},
			assertion: assert.NoError,
			want: TestElement{
				XMLName: xml.Name{Local: "element"},
				XSIType: XSIType{},
				Element: []TestElement{{
					XMLName: xml.Name{Local: "element"},
					XSIType: XSIType{Space: "http://example.com/type2", Local: "ExampleType"},
				}},
			},
		},
		{
			name: "empty_type",
			args: args{
				inXML: `<element xsi:type="">
						</element>`,
			},
			assertion: assert.NoError,
			want: TestElement{
				XMLName: xml.Name{Local: "element"},
				XSIType: XSIType{},
				Element: nil,
			},
		},
		{
			name: "default_ds_empty_type",
			args: args{
				inXML: `<element
							xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
							xmlns="http://example.com/type" xsi:type="">
						</element>`,
			},
			assertion: assert.NoError,
			want: TestElement{
				XMLName: xml.Name{Space: "http://example.com/type", Local: "element"},
				XSIType: XSIType{Space: "http://example.com/type"},
				Element: nil,
			},
		},
		{
			name: "colon_only",
			args: args{
				inXML: `<element xsi:type=":">
						</element>`,
			},
			assertion: assert.NoError,
			want: TestElement{
				XMLName: xml.Name{Local: "element"},
				XSIType: XSIType{},
				Element: nil,
			},
		},
		{
			name: "missed_prefix",
			args: args{
				inXML: `<element
							xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
							xsi:type="t:ExampleType">
						</element>`,
			},
			assertion: assert.NoError,
			want: TestElement{
				XMLName: xml.Name{Local: "element"},
				XSIType: XSIType{Space: "t", Local: "ExampleType"},
				Element: nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := xml.NewDecoder(strings.NewReader(tt.args.inXML))
			got := TestElement{}
			tt.assertion(t, xmlDecodeWithXSIType(d, &got))
			assert.Equal(t, tt.want, got)
		})
	}
}
