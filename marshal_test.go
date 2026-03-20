package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func BenchmarkMarshalString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		marshal("test")
	}
}

func TestUnmarshal(t *testing.T) {
	var (
		err            error
		valInt         int
		valInt8        int8
		valInt16       int16
		valInt32       int32
		valInt64       int64
		valString      string
		valTime        time.Time
		valUnsupported testing.T
		valFloat32     float32
		valFloat64     float64
		valArrayString []string
		valArrayInt    []int
		valArray       Array
	)

	err = unmarshal(&valInt, "10")
	assert.Equal(t, int(10), valInt)
	assert.NoError(t, err)

	err = unmarshal(&valInt8, "10")
	assert.Equal(t, int8(10), valInt8)
	assert.NoError(t, err)

	err = unmarshal(&valInt16, "10")
	assert.Equal(t, int16(10), valInt16)
	assert.NoError(t, err)

	err = unmarshal(&valInt32, "10")
	assert.Equal(t, int32(10), valInt32)
	assert.NoError(t, err)

	err = unmarshal(&valInt64, "10")
	assert.Equal(t, int64(10), valInt64)
	assert.NoError(t, err)

	err = unmarshal(&valString, "10")
	assert.Equal(t, "10", valString)
	assert.NoError(t, err)

	err = unmarshal(&valString, "String1\\'")
	assert.Equal(t, "String1'", valString)
	assert.NoError(t, err)

	err = unmarshal(&valTime, "2016-10-07 19:21:17")
	assert.Equal(t, time.Date(2016, 10, 7, 19, 21, 17, 0, time.UTC), valTime)
	assert.NoError(t, err)

	err = unmarshal(&valUnsupported, "10")
	assert.Error(t, err)

	err = unmarshal(&valFloat32, "3.141592")
	assert.Equal(t, float32(3.141592), valFloat32)
	assert.NoError(t, err)

	err = unmarshal(&valFloat64, "3.1415926535")
	assert.Equal(t, float64(3.1415926535), valFloat64)
	assert.NoError(t, err)

	err = unmarshal(&valArrayString, "['k10','20']")
	assert.Equal(t, []string{"k10", "20"}, valArrayString)
	assert.NoError(t, err)

	err = unmarshal(&valArrayString, "")
	assert.Error(t, err, "Column data is not of type []string")

	err = unmarshal(&valArrayString, "[]")
	assert.Equal(t, []string{}, valArrayString)
	assert.NoError(t, err)

	err = unmarshal(&valArrayInt, "[10,20]")
	assert.Equal(t, []int{10, 20}, valArrayInt)
	assert.NoError(t, err)

	err = unmarshal(&valArrayInt, "")
	assert.Error(t, err, "Column data is not of type []int")

	err = unmarshal(&valArrayInt, "[]")
	assert.Equal(t, []int{}, valArrayInt)
	assert.NoError(t, err)

	err = unmarshal(&valArray, "['k10','20']")
	assert.Equal(t, Array{"k10", "20"}, valArray)
	assert.NoError(t, err)

	err = unmarshal(&valArray, "[10,20]")
	assert.Equal(t, Array{10, 20}, valArray)
	assert.NoError(t, err)

	err = unmarshal(&valArray, "[3.14,5.25]")
	assert.Equal(t, Array{3.14, 5.25}, valArray)
	assert.NoError(t, err)

	err = unmarshal(&valArray, "")
	assert.Error(t, err, "Column data is not of type Array")

	err = unmarshal(&valArray, "[]")
	assert.Equal(t, Array{}, valArray)
	assert.NoError(t, err)
}

func TestUnmarshalUint(t *testing.T) {
	var (
		valUint   uint
		valUint8  uint8
		valUint16 uint16
		valUint32 uint32
		valUint64 uint64
	)

	err := unmarshal(&valUint, "42")
	assert.NoError(t, err)
	assert.Equal(t, uint(42), valUint)

	err = unmarshal(&valUint8, "255")
	assert.NoError(t, err)
	assert.Equal(t, uint8(255), valUint8)

	err = unmarshal(&valUint16, "65535")
	assert.NoError(t, err)
	assert.Equal(t, uint16(65535), valUint16)

	err = unmarshal(&valUint32, "4294967295")
	assert.NoError(t, err)
	assert.Equal(t, uint32(4294967295), valUint32)

	err = unmarshal(&valUint64, "18446744073709551615")
	assert.NoError(t, err)
	assert.Equal(t, uint64(18446744073709551615), valUint64)
}

func TestUnmarshalBool(t *testing.T) {
	var val bool

	err := unmarshal(&val, "1")
	assert.NoError(t, err)
	assert.True(t, val)

	err = unmarshal(&val, "0")
	assert.NoError(t, err)
	assert.False(t, val)

	err = unmarshal(&val, "true")
	assert.NoError(t, err)
	assert.True(t, val)

	err = unmarshal(&val, "false")
	assert.NoError(t, err)
	assert.False(t, val)

	err = unmarshal(&val, "invalid")
	assert.Error(t, err)
}

func TestUnmarshalNullable(t *testing.T) {
	var valStr *string
	err := unmarshal(&valStr, `\N`)
	assert.NoError(t, err)
	assert.Nil(t, valStr)

	err = unmarshal(&valStr, "hello")
	assert.NoError(t, err)
	assert.NotNil(t, valStr)
	assert.Equal(t, "hello", *valStr)

	var valInt *int64
	err = unmarshal(&valInt, `\N`)
	assert.NoError(t, err)
	assert.Nil(t, valInt)

	err = unmarshal(&valInt, "42")
	assert.NoError(t, err)
	assert.NotNil(t, valInt)
	assert.Equal(t, int64(42), *valInt)

	var valFloat *float64
	err = unmarshal(&valFloat, `\N`)
	assert.NoError(t, err)
	assert.Nil(t, valFloat)

	err = unmarshal(&valFloat, "3.14")
	assert.NoError(t, err)
	assert.NotNil(t, valFloat)
	assert.Equal(t, 3.14, *valFloat)
}

func TestUnmarshalDateOnly(t *testing.T) {
	var val time.Time
	err := unmarshal(&val, "2023-01-15")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC), val)
}

func TestMarshal(t *testing.T) {
	assert.Equal(t, "10", marshal(10))
	assert.Equal(t, "10", marshal(int8(10)))
	assert.Equal(t, "10", marshal(int16(10)))
	assert.Equal(t, "10", marshal(int32(10)))
	assert.Equal(t, "10", marshal(int64(10)))

	assert.Equal(t, "1", marshal(true))
	assert.Equal(t, "0", marshal(false))

	assert.Equal(t, "3.141592", marshal(float32(3.141592)))
	assert.Equal(t, "3.1415926535", marshal(float64(3.1415926535)))

	assert.Equal(t, "'10'", marshal("10"))
	assert.Equal(t, "'String1\\''", marshal("String1'"))
	assert.Equal(t, "'String\r'", marshal("String\r"))
	assert.Equal(t, "'String\r'", marshal("String\r"))
	assert.Equal(t, `'String\\'`, marshal(`String\`))
	assert.Equal(t, "[10,20,30]", marshal(Array{10, 20, 30}))
	assert.Equal(t, "['k10','20','30val']", marshal(Array{"k10", "20", "30val"}))
	assert.Equal(t, "['k10','20','30val']", marshal([]string{"k10", "20", "30val"}))
	assert.Equal(t, "['k10','20','30val\\\\']", marshal([]string{"k10", "20", "30val\\"}))
	assert.Equal(t, "[10,20,30]", marshal([]int{10, 20, 30}))
	assert.Equal(t, "IPv4StringToNum('192.0.2.128')", marshal(Func{"IPv4StringToNum", "192.0.2.128"}))
	assert.Equal(t, "IPv4NumToString(3221225985)", marshal(Func{"IPv4NumToString", 3221225985}))
	assert.Equal(t, "''", marshal(t))
	assert.Equal(t, "2017-04-10", marshal(time.Date(2017, 04, 10, 0, 0, 0, 0, time.UTC)))
}

func TestMarshalDateTime(t *testing.T) {
	assert.Equal(t, "2023-01-15 10:30:45", marshal(time.Date(2023, 1, 15, 10, 30, 45, 0, time.UTC)))
	assert.Equal(t, "2023-01-15", marshal(time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC)))
}

func TestMarshalUint(t *testing.T) {
	assert.Equal(t, "42", marshal(uint(42)))
	assert.Equal(t, "255", marshal(uint8(255)))
	assert.Equal(t, "65535", marshal(uint16(65535)))
	assert.Equal(t, "4294967295", marshal(uint32(4294967295)))
	assert.Equal(t, "18446744073709551615", marshal(uint64(18446744073709551615)))
}
