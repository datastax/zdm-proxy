package cloudgateproxy

import (
	"errors"
	"reflect"
	"testing"
)

func TestReadShort(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected interface{}
	}{
		{"simple short", []byte{0, 5}, uint16(5)},
		{"zero short", []byte{0, 0}, uint16(0)},
		{"cannot read short", []byte{0}, errors.New("not enough bytes to read a short")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := readShort(tt.data)
			if err != nil {
				if !reflect.DeepEqual(err, tt.expected) {
					t.Errorf("readShort() actual = %v, expected %v", err, tt.expected)
				}
			} else if actual != tt.expected {
				t.Errorf("readShort() actual = %v, expected %v", actual, tt.expected)
			}
		})
	}
}

func TestReadInt(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected interface{}
	}{
		{"simple int", []byte{0, 0, 0, 5}, uint32(5)},
		{"zero int", []byte{0, 0, 0, 0}, uint32(0)},
		{"cannot read int", []byte{0, 0, 0}, errors.New("not enough bytes to read an int")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := readInt(tt.data)
			if err != nil {
				if !reflect.DeepEqual(err, tt.expected) {
					t.Errorf("readInt() actual = %v, expected %v", err, tt.expected)
				}
			} else if actual != tt.expected {
				t.Errorf("readInt() actual = %v, expected %v", actual, tt.expected)
			}
		})
	}
}

func TestReadString(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected interface{}
	}{
		{"simple string", []byte{0, 5, 0x68, 0x65, 0x6c, 0x6c, 0x6f}, "hello"},
		{"empty string", []byte{0, 0}, ""},
		{"cannot read length", []byte{0}, errors.New("not enough bytes to read a short")},
		{"cannot read string", []byte{0, 5, 0x68, 0x65, 0x6c, 0x6c}, errors.New("not enough bytes to read a string")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := readString(tt.data)
			if err != nil {
				if !reflect.DeepEqual(err, tt.expected) {
					t.Errorf("readString() actual = %v, expected %v", err, tt.expected)
				}
			} else if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("readString() actual = %v, want %v", actual, tt.expected)
			}
		})
	}
}

func TestReadLongString(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected interface{}
	}{
		{"simple string", []byte{0, 0, 0, 5, 0x68, 0x65, 0x6c, 0x6c, 0x6f}, "hello"},
		{"empty string", []byte{0, 0, 0, 0}, ""},
		{"cannot read length", []byte{0, 0, 0}, errors.New("not enough bytes to read an int")},
		{"cannot read string", []byte{0, 0, 0, 5, 0x68, 0x65, 0x6c, 0x6c}, errors.New("not enough bytes to read a long string")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := readLongString(tt.data)
			if err != nil {
				if !reflect.DeepEqual(err, tt.expected) {
					t.Errorf("readLongString() actual = %v, expected %v", err, tt.expected)
				}
			} else if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("readLongString() actual = %v, want %v", actual, tt.expected)
			}
		})
	}
}

func TestReadShortBytes(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected interface{}
	}{
		{"simple array", []byte{0, 5, 1, 2, 3, 4, 5}, []byte{1, 2, 3, 4, 5}},
		{"empty array", []byte{0, 0}, []byte{}},
		{"cannot read length", []byte{0}, errors.New("not enough bytes to read a short")},
		{"cannot read array", []byte{0, 5, 1, 2, 3, 4}, errors.New("not enough bytes to read a short bytes")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := readShortBytes(tt.data)
			if err != nil {
				if !reflect.DeepEqual(err, tt.expected) {
					t.Errorf("readShortBytes() actual = %v, expected %v", err, tt.expected)
				}
			} else if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("readShortBytes() actual = %v, expected %v", actual, tt.expected)
			}
		})
	}
}
