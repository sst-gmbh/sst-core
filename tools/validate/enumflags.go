// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package validate

import (
	"fmt"

	"github.com/creachadair/goflags/enumflag"
)

type ValuesEnum interface {
	~int | ~uint | ~int8 | ~uint8 | ~int32 | ~uint16 | ~int16 | ~uint32 | ~int64 | ~uint64
}
type Values[E ValuesEnum] struct {
	firstEnumValue enumflag.Value
	other          []enumflag.Value
}

func ValuesOf[E ValuesEnum](defaultKey string, otherKeys ...string) Values[E] {
	return Values[E]{firstEnumValue: *enumflag.New(defaultKey, otherKeys...)}
}

func (v Values[E]) Type() string         { return "stringArray" }
func (v Values[E]) Help(h string) string { return v.firstEnumValue.Help(h) }

func (v Values[E]) String() string {
	keys := v.Keys()
	if len(keys) == 0 {
		return ""
	}
	return fmt.Sprintf("[%q]", keys)
}

func (v *Values[E]) Set(s string) error {
	if v.other == nil {
		err := v.firstEnumValue.Set(s)
		if err != nil {
			return err
		}
		v.other = []enumflag.Value{}
		return nil
	}
	e := v.firstEnumValue
	err := e.Set(s)
	if err != nil {
		return err
	}
	v.other = append(v.other, e)
	return nil
}

func (v *Values[E]) Keys() []string {
	if v.other == nil {
		return nil
	}
	keys := make([]string, 0, len(v.other)+1)
	keys = append(keys, v.firstEnumValue.Key())
	for _, e := range v.other {
		keys = append(keys, e.Key())
	}
	return keys
}

func (v *Values[E]) Enums() []E {
	if v.other == nil {
		return nil
	}
	enums := make([]E, 0, len(v.other)+1)
	enums = append(enums, E(v.firstEnumValue.Index()))
	for _, e := range v.other {
		enums = append(enums, E(e.Index()))
	}
	return enums
}

func (v *Values[E]) Len() int {
	if v.other == nil {
		return 0
	}
	return len(v.other) + 1
}
