package clickhouse

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const nullValue = `\N`

func escape(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return s
}

func unescape(s string) string {
	s = strings.ReplaceAll(s, `\\`, `\`)
	s = strings.ReplaceAll(s, `\'`, `'`)
	return s
}

func isArray(s string) bool {
	return strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]")
}

func isEmptyArray(s string) bool {
	return s == "[]"
}

func splitStringToItems(s string) []string {
	return strings.Split(s[1:len(s)-1], ",")
}

func unmarshal(value any, data string) error {
	// Nullable types: \N represents NULL.
	if data == nullValue {
		return nil
	}

	switch v := value.(type) {
	case *int:
		n, err := strconv.Atoi(data)
		if err != nil {
			return err
		}
		*v = n
	case *int8:
		n, err := strconv.ParseInt(data, 10, 8)
		if err != nil {
			return err
		}
		*v = int8(n)
	case *int16:
		n, err := strconv.ParseInt(data, 10, 16)
		if err != nil {
			return err
		}
		*v = int16(n)
	case *int32:
		n, err := strconv.ParseInt(data, 10, 32)
		if err != nil {
			return err
		}
		*v = int32(n)
	case *int64:
		n, err := strconv.ParseInt(data, 10, 64)
		if err != nil {
			return err
		}
		*v = n
	case *uint:
		n, err := strconv.ParseUint(data, 10, 64)
		if err != nil {
			return err
		}
		*v = uint(n)
	case *uint8:
		n, err := strconv.ParseUint(data, 10, 8)
		if err != nil {
			return err
		}
		*v = uint8(n)
	case *uint16:
		n, err := strconv.ParseUint(data, 10, 16)
		if err != nil {
			return err
		}
		*v = uint16(n)
	case *uint32:
		n, err := strconv.ParseUint(data, 10, 32)
		if err != nil {
			return err
		}
		*v = uint32(n)
	case *uint64:
		n, err := strconv.ParseUint(data, 10, 64)
		if err != nil {
			return err
		}
		*v = n
	case *float32:
		n, err := strconv.ParseFloat(data, 32)
		if err != nil {
			return err
		}
		*v = float32(n)
	case *float64:
		n, err := strconv.ParseFloat(data, 64)
		if err != nil {
			return err
		}
		*v = n
	case *bool:
		switch data {
		case "1", "true", "True":
			*v = true
		case "0", "false", "False":
			*v = false
		default:
			return fmt.Errorf("cannot unmarshal %q into bool", data)
		}
	case *string:
		*v = unescape(data)
	case *time.Time:
		// Try DateTime first, then Date.
		t, err := time.ParseInLocation("2006-01-02 15:04:05", data, time.UTC)
		if err != nil {
			t, err = time.ParseInLocation("2006-01-02", data, time.UTC)
			if err != nil {
				return err
			}
		}
		*v = t
	case **string:
		s := unescape(data)
		*v = &s
	case **int64:
		n, err := strconv.ParseInt(data, 10, 64)
		if err != nil {
			return err
		}
		*v = &n
	case **float64:
		f, err := strconv.ParseFloat(data, 64)
		if err != nil {
			return err
		}
		*v = &f
	case *[]int:
		return unmarshalIntSlice(v, data)
	case *[]string:
		return unmarshalStringSlice(v, data)
	case *Array:
		return unmarshalArray(v, data)
	default:
		return fmt.Errorf("unsupported type %T for unmarshaling", v)
	}
	return nil
}

func unmarshalIntSlice(v *[]int, data string) error {
	if !isArray(data) {
		return fmt.Errorf("column data is not of type []int")
	}
	if isEmptyArray(data) {
		*v = []int{}
		return nil
	}
	items := splitStringToItems(data)
	res := make([]int, len(items))
	for i, item := range items {
		if err := unmarshal(&res[i], item); err != nil {
			return err
		}
	}
	*v = res
	return nil
}

func unmarshalStringSlice(v *[]string, data string) error {
	if !isArray(data) {
		return fmt.Errorf("column data is not of type []string")
	}
	if isEmptyArray(data) {
		*v = []string{}
		return nil
	}
	items := splitStringToItems(data)
	res := make([]string, len(items))
	for i, item := range items {
		var s string
		if err := unmarshal(&s, item); err != nil {
			return err
		}
		res[i] = s[1 : len(s)-1]
	}
	*v = res
	return nil
}

func unmarshalArray(v *Array, data string) error {
	if !isArray(data) {
		return fmt.Errorf("column data is not of type Array")
	}
	if isEmptyArray(data) {
		*v = Array{}
		return nil
	}

	items := splitStringToItems(data)
	res := make(Array, len(items))

	// Try int first.
	var intval int
	if err := unmarshal(&intval, items[0]); err == nil {
		for i, item := range items {
			unmarshal(&intval, item)
			res[i] = intval
		}
		*v = res
		return nil
	}

	// Try float64.
	var floatval float64
	if err := unmarshal(&floatval, items[0]); err == nil {
		for i, item := range items {
			unmarshal(&floatval, item)
			res[i] = floatval
		}
		*v = res
		return nil
	}

	// Fall back to string.
	var stringval string
	if err := unmarshal(&stringval, items[0]); err == nil {
		for i, item := range items {
			unmarshal(&stringval, item)
			res[i] = stringval[1 : len(stringval)-1]
		}
		*v = res
		return nil
	}

	return fmt.Errorf("cannot determine array element type")
}

func marshal(value any) string {
	if reflect.TypeOf(value).Kind() == reflect.Slice {
		v := reflect.ValueOf(value)
		res := make([]string, v.Len())
		for i := range res {
			res[i] = marshal(v.Index(i).Interface())
		}
		return "[" + strings.Join(res, ",") + "]"
	}
	if t := reflect.TypeOf(value); t.Kind() == reflect.Struct && strings.HasSuffix(t.String(), "Func") {
		f := value.(Func)
		return fmt.Sprintf("%s(%v)", f.Name, marshal(f.Args))
	}
	switch v := value.(type) {
	case string:
		return "'" + escape(v) + "'"
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return fmt.Sprintf("%v", v)
	case bool:
		if v {
			return "1"
		}
		return "0"
	case time.Time:
		if v.Hour() == 0 && v.Minute() == 0 && v.Second() == 0 && v.Nanosecond() == 0 {
			return v.Format("2006-01-02")
		}
		return v.Format("2006-01-02 15:04:05")
	}
	return "''"
}
