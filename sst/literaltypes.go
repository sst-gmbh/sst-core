// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"fmt"
	"strconv"
)

// Literal is an [Term] that represents a literal value.
// The Literal provides method DataType that returns it's
// type as an IBNode and method Value that returns it's
// of unspecified type.
//
// There exists specialized interfaces for optimized access
// of literal value. These are:
//   - type [StringLiteral] for xsd:string
//   - type [LangStringLiteral] for rdf:langString
//   - type [DoubleLiteral] for xsd:double
//   - type [IntegerLiteral] for xsd:integer
//   - type [BooleanLiteral] for xsd:boolean
//
// Applications should never expect concrete type as literal value
// but use this interface of it's specialization interfaces to
// access literal value or data type.
type Literal interface {
	Term

	// DataType returns the DataType of this Literal as an IBNode.
	DataType() IBNode

	apiValue() interface{}
}

// literalToObject converts a literal to an Object.
func literalToObject(l *literal) Term {
	switch l.typeOf {
	case &literalTypeString.ibNode:
		return String(l.value)
	case &literalTypeDouble.ibNode:
		return Double(float64FromBytes(l.value))
	case &literalTypeFloat.ibNode:
		return Float(float32FromBytes(l.value))
	case &literalTypeInteger.ibNode:
		return Integer(int64FromBytes(l.value))
	case &literalTypeBoolean.ibNode:
		return Boolean(l.value == "t")
	case &literalTypeLangString.ibNode:
		// last two bytes are language tag
		return LangString{Val: l.value[:len(l.value)-2], LangTag: l.value[len(l.value)-2:]}
	case &literalTypeByte.ibNode:
		return Byte(int8FromBytes(l.value))
	case &literalTypeShort.ibNode:
		return Short(int16FromBytes(l.value))
	case &literalTypeInt.ibNode:
		return Int(int32FromBytes(l.value))
	case &literalTypeLong.ibNode:
		return Long(int64FromBytes(l.value))
	case &literalTypeUnsignedByte.ibNode:
		return UnsignedByte(uint8FromBytes(l.value))
	case &literalTypeUnsignedShort.ibNode:
		return UnsignedShort(uint16FromBytes(l.value))
	case &literalTypeUnsignedInt.ibNode:
		return UnsignedInt(uint32FromBytes(l.value))
	case &literalTypeUnsignedLong.ibNode:
		return UnsignedLong(uint64FromBytes(l.value))
	case &literalTypeDateTime.ibNode:
		return TypedString{Val: l.value, Type: &literalTypeDateTime.ibNode}
	case &literalTypeDateTimeStamp.ibNode:
		return TypedString{Val: l.value, Type: &literalTypeDateTimeStamp.ibNode}
	default:
		panic("unknown literal type")
	}
}

func literalToString(l Literal) string {
	switch o := l.(type) {
	case String:
		return "\"" + string(o) + "\""
	case LangString:
		return "\"" + string(o.Val) + "\"" + "@" + string(o.LangTag)
	case Double:
		return strconv.FormatFloat(float64(o), 'f', 2, 64)
	case Float:
		return strconv.FormatFloat(float64(o), 'f', -1, 32)
	case Integer:
		return strconv.FormatInt(int64(o), 10)
	case Boolean:
		return strconv.FormatBool(bool(o))
	case Byte:
		return strconv.FormatInt(int64(o), 10)
	case Short:
		return strconv.FormatInt(int64(o), 10)
	case Int:
		return strconv.FormatInt(int64(o), 10)
	case Long:
		return strconv.FormatInt(int64(o), 10)
	case UnsignedByte:
		return strconv.FormatUint(uint64(o), 10)
	case UnsignedShort:
		return strconv.FormatUint(uint64(o), 10)
	case UnsignedInt:
		return strconv.FormatUint(uint64(o), 10)
	case UnsignedLong:
		return strconv.FormatUint(uint64(o), 10)
	case *literal:
		return literalToString(literalToObject(o).(Literal))
	default:
		return fmt.Errorf("unknown literal type %T", o).Error()
	}
}
