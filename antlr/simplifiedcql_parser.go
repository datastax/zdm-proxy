// Code generated from antlr/SimplifiedCql.g4 by ANTLR 4.8. DO NOT EDIT.

package parser // SimplifiedCql

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/antlr/antlr4/runtime/Go/antlr"
)

// Suppress unused import errors
var _ = fmt.Printf
var _ = reflect.Copy
var _ = strconv.Itoa

var parserATN = []uint16{
	3, 24715, 42794, 33075, 47597, 16764, 15335, 30598, 22884, 3, 63, 281,
	4, 2, 9, 2, 4, 3, 9, 3, 4, 4, 9, 4, 4, 5, 9, 5, 4, 6, 9, 6, 4, 7, 9, 7,
	4, 8, 9, 8, 4, 9, 9, 9, 4, 10, 9, 10, 4, 11, 9, 11, 4, 12, 9, 12, 4, 13,
	9, 13, 4, 14, 9, 14, 4, 15, 9, 15, 4, 16, 9, 16, 4, 17, 9, 17, 4, 18, 9,
	18, 4, 19, 9, 19, 4, 20, 9, 20, 4, 21, 9, 21, 4, 22, 9, 22, 4, 23, 9, 23,
	4, 24, 9, 24, 4, 25, 9, 25, 4, 26, 9, 26, 4, 27, 9, 27, 4, 28, 9, 28, 3,
	2, 3, 2, 5, 2, 59, 10, 2, 3, 2, 3, 2, 5, 2, 63, 10, 2, 5, 2, 65, 10, 2,
	3, 3, 3, 3, 5, 3, 69, 10, 3, 3, 3, 5, 3, 72, 10, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 4, 3, 4, 3, 5, 3, 5, 3, 5, 7, 5, 84, 10, 5, 12, 5, 14, 5, 87,
	11, 5, 3, 5, 5, 5, 90, 10, 5, 3, 6, 3, 6, 3, 6, 5, 6, 95, 10, 6, 3, 7,
	3, 7, 3, 7, 3, 7, 3, 7, 3, 7, 3, 7, 3, 7, 3, 7, 5, 7, 106, 10, 7, 3, 8,
	3, 8, 3, 8, 3, 8, 3, 8, 3, 8, 3, 8, 5, 8, 115, 10, 8, 3, 9, 3, 9, 3, 9,
	5, 9, 120, 10, 9, 3, 10, 3, 10, 3, 10, 3, 10, 3, 10, 3, 10, 3, 10, 3, 10,
	5, 10, 130, 10, 10, 3, 10, 3, 10, 5, 10, 134, 10, 10, 3, 10, 5, 10, 137,
	10, 10, 3, 11, 3, 11, 3, 11, 5, 11, 142, 10, 11, 3, 12, 3, 12, 3, 12, 3,
	12, 7, 12, 148, 10, 12, 12, 12, 14, 12, 151, 11, 12, 5, 12, 153, 10, 12,
	3, 12, 3, 12, 3, 13, 3, 13, 3, 13, 3, 13, 7, 13, 161, 10, 13, 12, 13, 14,
	13, 164, 11, 13, 5, 13, 166, 10, 13, 3, 13, 3, 13, 3, 14, 3, 14, 3, 14,
	3, 14, 3, 14, 3, 14, 3, 14, 3, 14, 3, 14, 7, 14, 179, 10, 14, 12, 14, 14,
	14, 182, 11, 14, 5, 14, 184, 10, 14, 3, 14, 3, 14, 3, 15, 3, 15, 3, 15,
	3, 15, 3, 15, 3, 15, 3, 15, 3, 15, 3, 15, 5, 15, 197, 10, 15, 3, 16, 3,
	16, 3, 17, 3, 17, 3, 17, 3, 17, 3, 17, 3, 17, 3, 17, 3, 17, 3, 17, 3, 17,
	3, 17, 3, 17, 3, 17, 3, 17, 3, 17, 3, 17, 3, 17, 5, 17, 218, 10, 17, 3,
	18, 3, 18, 3, 18, 3, 18, 3, 18, 7, 18, 225, 10, 18, 12, 18, 14, 18, 228,
	11, 18, 3, 18, 3, 18, 3, 19, 3, 19, 3, 19, 3, 19, 3, 19, 3, 19, 3, 19,
	3, 19, 3, 19, 3, 19, 3, 19, 3, 19, 3, 19, 3, 19, 5, 19, 246, 10, 19, 3,
	20, 3, 20, 3, 20, 7, 20, 251, 10, 20, 12, 20, 14, 20, 254, 11, 20, 3, 21,
	3, 21, 3, 22, 3, 22, 3, 23, 3, 23, 3, 24, 3, 24, 3, 25, 3, 25, 3, 25, 5,
	25, 267, 10, 25, 3, 25, 3, 25, 3, 26, 3, 26, 3, 27, 7, 27, 274, 10, 27,
	12, 27, 14, 27, 277, 11, 27, 3, 28, 3, 28, 3, 28, 2, 2, 29, 2, 4, 6, 8,
	10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44,
	46, 48, 50, 52, 54, 2, 4, 9, 2, 17, 20, 22, 24, 26, 28, 31, 31, 33, 33,
	41, 46, 48, 50, 4, 2, 52, 52, 57, 57, 2, 297, 2, 64, 3, 2, 2, 2, 4, 66,
	3, 2, 2, 2, 6, 78, 3, 2, 2, 2, 8, 89, 3, 2, 2, 2, 10, 91, 3, 2, 2, 2, 12,
	105, 3, 2, 2, 2, 14, 114, 3, 2, 2, 2, 16, 119, 3, 2, 2, 2, 18, 136, 3,
	2, 2, 2, 20, 141, 3, 2, 2, 2, 22, 143, 3, 2, 2, 2, 24, 156, 3, 2, 2, 2,
	26, 169, 3, 2, 2, 2, 28, 196, 3, 2, 2, 2, 30, 198, 3, 2, 2, 2, 32, 217,
	3, 2, 2, 2, 34, 219, 3, 2, 2, 2, 36, 245, 3, 2, 2, 2, 38, 247, 3, 2, 2,
	2, 40, 255, 3, 2, 2, 2, 42, 257, 3, 2, 2, 2, 44, 259, 3, 2, 2, 2, 46, 261,
	3, 2, 2, 2, 48, 266, 3, 2, 2, 2, 50, 270, 3, 2, 2, 2, 52, 275, 3, 2, 2,
	2, 54, 278, 3, 2, 2, 2, 56, 58, 5, 4, 3, 2, 57, 59, 7, 63, 2, 2, 58, 57,
	3, 2, 2, 2, 58, 59, 3, 2, 2, 2, 59, 65, 3, 2, 2, 2, 60, 62, 5, 6, 4, 2,
	61, 63, 7, 63, 2, 2, 62, 61, 3, 2, 2, 2, 62, 63, 3, 2, 2, 2, 63, 65, 3,
	2, 2, 2, 64, 56, 3, 2, 2, 2, 64, 60, 3, 2, 2, 2, 65, 3, 3, 2, 2, 2, 66,
	68, 7, 39, 2, 2, 67, 69, 7, 34, 2, 2, 68, 67, 3, 2, 2, 2, 68, 69, 3, 2,
	2, 2, 69, 71, 3, 2, 2, 2, 70, 72, 7, 25, 2, 2, 71, 70, 3, 2, 2, 2, 71,
	72, 3, 2, 2, 2, 72, 73, 3, 2, 2, 2, 73, 74, 5, 8, 5, 2, 74, 75, 7, 29,
	2, 2, 75, 76, 5, 40, 21, 2, 76, 77, 5, 52, 27, 2, 77, 5, 3, 2, 2, 2, 78,
	79, 5, 52, 27, 2, 79, 7, 3, 2, 2, 2, 80, 85, 5, 10, 6, 2, 81, 82, 7, 3,
	2, 2, 82, 84, 5, 10, 6, 2, 83, 81, 3, 2, 2, 2, 84, 87, 3, 2, 2, 2, 85,
	83, 3, 2, 2, 2, 85, 86, 3, 2, 2, 2, 86, 90, 3, 2, 2, 2, 87, 85, 3, 2, 2,
	2, 88, 90, 7, 4, 2, 2, 89, 80, 3, 2, 2, 2, 89, 88, 3, 2, 2, 2, 90, 9, 3,
	2, 2, 2, 91, 94, 5, 12, 7, 2, 92, 93, 7, 16, 2, 2, 93, 95, 5, 50, 26, 2,
	94, 92, 3, 2, 2, 2, 94, 95, 3, 2, 2, 2, 95, 11, 3, 2, 2, 2, 96, 106, 5,
	50, 26, 2, 97, 106, 5, 14, 8, 2, 98, 99, 7, 21, 2, 2, 99, 100, 7, 5, 2,
	2, 100, 101, 5, 12, 7, 2, 101, 102, 7, 16, 2, 2, 102, 103, 5, 30, 16, 2,
	103, 104, 7, 6, 2, 2, 104, 106, 3, 2, 2, 2, 105, 96, 3, 2, 2, 2, 105, 97,
	3, 2, 2, 2, 105, 98, 3, 2, 2, 2, 106, 13, 3, 2, 2, 2, 107, 115, 5, 16,
	9, 2, 108, 115, 5, 36, 19, 2, 109, 110, 7, 5, 2, 2, 110, 111, 5, 28, 15,
	2, 111, 112, 7, 6, 2, 2, 112, 113, 5, 14, 8, 2, 113, 115, 3, 2, 2, 2, 114,
	107, 3, 2, 2, 2, 114, 108, 3, 2, 2, 2, 114, 109, 3, 2, 2, 2, 115, 15, 3,
	2, 2, 2, 116, 120, 5, 18, 10, 2, 117, 120, 5, 20, 11, 2, 118, 120, 7, 38,
	2, 2, 119, 116, 3, 2, 2, 2, 119, 117, 3, 2, 2, 2, 119, 118, 3, 2, 2, 2,
	120, 17, 3, 2, 2, 2, 121, 137, 7, 51, 2, 2, 122, 137, 7, 53, 2, 2, 123,
	137, 7, 54, 2, 2, 124, 137, 7, 55, 2, 2, 125, 137, 7, 56, 2, 2, 126, 137,
	7, 59, 2, 2, 127, 137, 7, 58, 2, 2, 128, 130, 7, 7, 2, 2, 129, 128, 3,
	2, 2, 2, 129, 130, 3, 2, 2, 2, 130, 131, 3, 2, 2, 2, 131, 137, 7, 37, 2,
	2, 132, 134, 7, 7, 2, 2, 133, 132, 3, 2, 2, 2, 133, 134, 3, 2, 2, 2, 134,
	135, 3, 2, 2, 2, 135, 137, 7, 32, 2, 2, 136, 121, 3, 2, 2, 2, 136, 122,
	3, 2, 2, 2, 136, 123, 3, 2, 2, 2, 136, 124, 3, 2, 2, 2, 136, 125, 3, 2,
	2, 2, 136, 126, 3, 2, 2, 2, 136, 127, 3, 2, 2, 2, 136, 129, 3, 2, 2, 2,
	136, 133, 3, 2, 2, 2, 137, 19, 3, 2, 2, 2, 138, 142, 5, 22, 12, 2, 139,
	142, 5, 24, 13, 2, 140, 142, 5, 26, 14, 2, 141, 138, 3, 2, 2, 2, 141, 139,
	3, 2, 2, 2, 141, 140, 3, 2, 2, 2, 142, 21, 3, 2, 2, 2, 143, 152, 7, 8,
	2, 2, 144, 149, 5, 14, 8, 2, 145, 146, 7, 3, 2, 2, 146, 148, 5, 14, 8,
	2, 147, 145, 3, 2, 2, 2, 148, 151, 3, 2, 2, 2, 149, 147, 3, 2, 2, 2, 149,
	150, 3, 2, 2, 2, 150, 153, 3, 2, 2, 2, 151, 149, 3, 2, 2, 2, 152, 144,
	3, 2, 2, 2, 152, 153, 3, 2, 2, 2, 153, 154, 3, 2, 2, 2, 154, 155, 7, 9,
	2, 2, 155, 23, 3, 2, 2, 2, 156, 165, 7, 10, 2, 2, 157, 162, 5, 14, 8, 2,
	158, 159, 7, 3, 2, 2, 159, 161, 5, 14, 8, 2, 160, 158, 3, 2, 2, 2, 161,
	164, 3, 2, 2, 2, 162, 160, 3, 2, 2, 2, 162, 163, 3, 2, 2, 2, 163, 166,
	3, 2, 2, 2, 164, 162, 3, 2, 2, 2, 165, 157, 3, 2, 2, 2, 165, 166, 3, 2,
	2, 2, 166, 167, 3, 2, 2, 2, 167, 168, 7, 11, 2, 2, 168, 25, 3, 2, 2, 2,
	169, 183, 7, 10, 2, 2, 170, 171, 5, 14, 8, 2, 171, 172, 7, 12, 2, 2, 172,
	180, 5, 14, 8, 2, 173, 174, 7, 3, 2, 2, 174, 175, 5, 14, 8, 2, 175, 176,
	7, 12, 2, 2, 176, 177, 5, 14, 8, 2, 177, 179, 3, 2, 2, 2, 178, 173, 3,
	2, 2, 2, 179, 182, 3, 2, 2, 2, 180, 178, 3, 2, 2, 2, 180, 181, 3, 2, 2,
	2, 181, 184, 3, 2, 2, 2, 182, 180, 3, 2, 2, 2, 183, 170, 3, 2, 2, 2, 183,
	184, 3, 2, 2, 2, 184, 185, 3, 2, 2, 2, 185, 186, 7, 11, 2, 2, 186, 27,
	3, 2, 2, 2, 187, 197, 5, 30, 16, 2, 188, 197, 5, 32, 17, 2, 189, 197, 5,
	34, 18, 2, 190, 197, 5, 44, 23, 2, 191, 192, 7, 30, 2, 2, 192, 193, 7,
	13, 2, 2, 193, 194, 5, 28, 15, 2, 194, 195, 7, 14, 2, 2, 195, 197, 3, 2,
	2, 2, 196, 187, 3, 2, 2, 2, 196, 188, 3, 2, 2, 2, 196, 189, 3, 2, 2, 2,
	196, 190, 3, 2, 2, 2, 196, 191, 3, 2, 2, 2, 197, 29, 3, 2, 2, 2, 198, 199,
	9, 2, 2, 2, 199, 31, 3, 2, 2, 2, 200, 201, 7, 35, 2, 2, 201, 202, 7, 13,
	2, 2, 202, 203, 5, 28, 15, 2, 203, 204, 7, 14, 2, 2, 204, 218, 3, 2, 2,
	2, 205, 206, 7, 40, 2, 2, 206, 207, 7, 13, 2, 2, 207, 208, 5, 28, 15, 2,
	208, 209, 7, 14, 2, 2, 209, 218, 3, 2, 2, 2, 210, 211, 7, 36, 2, 2, 211,
	212, 7, 13, 2, 2, 212, 213, 5, 28, 15, 2, 213, 214, 7, 3, 2, 2, 214, 215,
	5, 28, 15, 2, 215, 216, 7, 14, 2, 2, 216, 218, 3, 2, 2, 2, 217, 200, 3,
	2, 2, 2, 217, 205, 3, 2, 2, 2, 217, 210, 3, 2, 2, 2, 218, 33, 3, 2, 2,
	2, 219, 220, 7, 47, 2, 2, 220, 221, 7, 13, 2, 2, 221, 226, 5, 28, 15, 2,
	222, 223, 7, 3, 2, 2, 223, 225, 5, 28, 15, 2, 224, 222, 3, 2, 2, 2, 225,
	228, 3, 2, 2, 2, 226, 224, 3, 2, 2, 2, 226, 227, 3, 2, 2, 2, 227, 229,
	3, 2, 2, 2, 228, 226, 3, 2, 2, 2, 229, 230, 7, 14, 2, 2, 230, 35, 3, 2,
	2, 2, 231, 232, 5, 42, 22, 2, 232, 233, 7, 5, 2, 2, 233, 234, 7, 6, 2,
	2, 234, 246, 3, 2, 2, 2, 235, 236, 5, 42, 22, 2, 236, 237, 7, 5, 2, 2,
	237, 238, 7, 4, 2, 2, 238, 239, 7, 6, 2, 2, 239, 246, 3, 2, 2, 2, 240,
	241, 5, 42, 22, 2, 241, 242, 7, 5, 2, 2, 242, 243, 5, 38, 20, 2, 243, 244,
	7, 6, 2, 2, 244, 246, 3, 2, 2, 2, 245, 231, 3, 2, 2, 2, 245, 235, 3, 2,
	2, 2, 245, 240, 3, 2, 2, 2, 246, 37, 3, 2, 2, 2, 247, 252, 5, 12, 7, 2,
	248, 249, 7, 3, 2, 2, 249, 251, 5, 12, 7, 2, 250, 248, 3, 2, 2, 2, 251,
	254, 3, 2, 2, 2, 252, 250, 3, 2, 2, 2, 252, 253, 3, 2, 2, 2, 253, 39, 3,
	2, 2, 2, 254, 252, 3, 2, 2, 2, 255, 256, 5, 48, 25, 2, 256, 41, 3, 2, 2,
	2, 257, 258, 5, 48, 25, 2, 258, 43, 3, 2, 2, 2, 259, 260, 5, 48, 25, 2,
	260, 45, 3, 2, 2, 2, 261, 262, 5, 50, 26, 2, 262, 47, 3, 2, 2, 2, 263,
	264, 5, 46, 24, 2, 264, 265, 7, 15, 2, 2, 265, 267, 3, 2, 2, 2, 266, 263,
	3, 2, 2, 2, 266, 267, 3, 2, 2, 2, 267, 268, 3, 2, 2, 2, 268, 269, 5, 50,
	26, 2, 269, 49, 3, 2, 2, 2, 270, 271, 9, 3, 2, 2, 271, 51, 3, 2, 2, 2,
	272, 274, 5, 54, 28, 2, 273, 272, 3, 2, 2, 2, 274, 277, 3, 2, 2, 2, 275,
	273, 3, 2, 2, 2, 275, 276, 3, 2, 2, 2, 276, 53, 3, 2, 2, 2, 277, 275, 3,
	2, 2, 2, 278, 279, 11, 2, 2, 2, 279, 55, 3, 2, 2, 2, 30, 58, 62, 64, 68,
	71, 85, 89, 94, 105, 114, 119, 129, 133, 136, 141, 149, 152, 162, 165,
	180, 183, 196, 217, 226, 245, 252, 266, 275,
}
var deserializer = antlr.NewATNDeserializer(nil)
var deserializedATN = deserializer.DeserializeFromUInt16(parserATN)

var literalNames = []string{
	"", "','", "'*'", "'('", "')'", "'-'", "'['", "']'", "'{'", "'}'", "':'",
	"'<'", "'>'", "'.'", "", "", "", "", "", "", "", "", "", "", "", "", "",
	"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
	"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "';'",
}
var symbolicNames = []string{
	"", "", "", "", "", "", "", "", "", "", "", "", "", "", "K_AS", "K_ASCII",
	"K_BIGINT", "K_BLOB", "K_BOOLEAN", "K_CAST", "K_COUNTER", "K_DATE", "K_DECIMAL",
	"K_DISTINCT", "K_DOUBLE", "K_DURATION", "K_FLOAT", "K_FROM", "K_FROZEN",
	"K_INET", "K_INFINITY", "K_INT", "K_JSON", "K_LIST", "K_MAP", "K_NAN",
	"K_NULL", "K_SELECT", "K_SET", "K_SMALLINT", "K_TEXT", "K_TIME", "K_TIMESTAMP",
	"K_TIMEUUID", "K_TINYINT", "K_TUPLE", "K_UUID", "K_VARCHAR", "K_VARINT",
	"STRING_LITERAL", "QUOTED_IDENTIFIER", "INTEGER", "FLOAT", "BOOLEAN", "DURATION",
	"UNQUOTED_IDENTIFIER", "HEXNUMBER", "UUID", "WS", "COMMENT", "MULTILINE_COMMENT",
	"EOS",
}

var ruleNames = []string{
	"cqlStatement", "selectStatement", "otherStatement", "selectClause", "selector",
	"unaliasedSelector", "term", "literal", "primitiveLiteral", "collectionLiteral",
	"listLiteral", "setLiteral", "mapLiteral", "cqlType", "primitiveType",
	"collectionType", "tupleType", "functionCall", "functionArgs", "tableName",
	"functionName", "userTypeName", "keyspaceName", "qualifiedIdentifier",
	"identifier", "discardedContent", "unknown",
}
var decisionToDFA = make([]*antlr.DFA, len(deserializedATN.DecisionToState))

func init() {
	for index, ds := range deserializedATN.DecisionToState {
		decisionToDFA[index] = antlr.NewDFA(ds, index)
	}
}

type SimplifiedCqlParser struct {
	*antlr.BaseParser
}

func NewSimplifiedCqlParser(input antlr.TokenStream) *SimplifiedCqlParser {
	this := new(SimplifiedCqlParser)

	this.BaseParser = antlr.NewBaseParser(input)

	this.Interpreter = antlr.NewParserATNSimulator(this, deserializedATN, decisionToDFA, antlr.NewPredictionContextCache())
	this.RuleNames = ruleNames
	this.LiteralNames = literalNames
	this.SymbolicNames = symbolicNames
	this.GrammarFileName = "SimplifiedCql.g4"

	return this
}

// SimplifiedCqlParser tokens.
const (
	SimplifiedCqlParserEOF                 = antlr.TokenEOF
	SimplifiedCqlParserT__0                = 1
	SimplifiedCqlParserT__1                = 2
	SimplifiedCqlParserT__2                = 3
	SimplifiedCqlParserT__3                = 4
	SimplifiedCqlParserT__4                = 5
	SimplifiedCqlParserT__5                = 6
	SimplifiedCqlParserT__6                = 7
	SimplifiedCqlParserT__7                = 8
	SimplifiedCqlParserT__8                = 9
	SimplifiedCqlParserT__9                = 10
	SimplifiedCqlParserT__10               = 11
	SimplifiedCqlParserT__11               = 12
	SimplifiedCqlParserT__12               = 13
	SimplifiedCqlParserK_AS                = 14
	SimplifiedCqlParserK_ASCII             = 15
	SimplifiedCqlParserK_BIGINT            = 16
	SimplifiedCqlParserK_BLOB              = 17
	SimplifiedCqlParserK_BOOLEAN           = 18
	SimplifiedCqlParserK_CAST              = 19
	SimplifiedCqlParserK_COUNTER           = 20
	SimplifiedCqlParserK_DATE              = 21
	SimplifiedCqlParserK_DECIMAL           = 22
	SimplifiedCqlParserK_DISTINCT          = 23
	SimplifiedCqlParserK_DOUBLE            = 24
	SimplifiedCqlParserK_DURATION          = 25
	SimplifiedCqlParserK_FLOAT             = 26
	SimplifiedCqlParserK_FROM              = 27
	SimplifiedCqlParserK_FROZEN            = 28
	SimplifiedCqlParserK_INET              = 29
	SimplifiedCqlParserK_INFINITY          = 30
	SimplifiedCqlParserK_INT               = 31
	SimplifiedCqlParserK_JSON              = 32
	SimplifiedCqlParserK_LIST              = 33
	SimplifiedCqlParserK_MAP               = 34
	SimplifiedCqlParserK_NAN               = 35
	SimplifiedCqlParserK_NULL              = 36
	SimplifiedCqlParserK_SELECT            = 37
	SimplifiedCqlParserK_SET               = 38
	SimplifiedCqlParserK_SMALLINT          = 39
	SimplifiedCqlParserK_TEXT              = 40
	SimplifiedCqlParserK_TIME              = 41
	SimplifiedCqlParserK_TIMESTAMP         = 42
	SimplifiedCqlParserK_TIMEUUID          = 43
	SimplifiedCqlParserK_TINYINT           = 44
	SimplifiedCqlParserK_TUPLE             = 45
	SimplifiedCqlParserK_UUID              = 46
	SimplifiedCqlParserK_VARCHAR           = 47
	SimplifiedCqlParserK_VARINT            = 48
	SimplifiedCqlParserSTRING_LITERAL      = 49
	SimplifiedCqlParserQUOTED_IDENTIFIER   = 50
	SimplifiedCqlParserINTEGER             = 51
	SimplifiedCqlParserFLOAT               = 52
	SimplifiedCqlParserBOOLEAN             = 53
	SimplifiedCqlParserDURATION            = 54
	SimplifiedCqlParserUNQUOTED_IDENTIFIER = 55
	SimplifiedCqlParserHEXNUMBER           = 56
	SimplifiedCqlParserUUID                = 57
	SimplifiedCqlParserWS                  = 58
	SimplifiedCqlParserCOMMENT             = 59
	SimplifiedCqlParserMULTILINE_COMMENT   = 60
	SimplifiedCqlParserEOS                 = 61
)

// SimplifiedCqlParser rules.
const (
	SimplifiedCqlParserRULE_cqlStatement        = 0
	SimplifiedCqlParserRULE_selectStatement     = 1
	SimplifiedCqlParserRULE_otherStatement      = 2
	SimplifiedCqlParserRULE_selectClause        = 3
	SimplifiedCqlParserRULE_selector            = 4
	SimplifiedCqlParserRULE_unaliasedSelector   = 5
	SimplifiedCqlParserRULE_term                = 6
	SimplifiedCqlParserRULE_literal             = 7
	SimplifiedCqlParserRULE_primitiveLiteral    = 8
	SimplifiedCqlParserRULE_collectionLiteral   = 9
	SimplifiedCqlParserRULE_listLiteral         = 10
	SimplifiedCqlParserRULE_setLiteral          = 11
	SimplifiedCqlParserRULE_mapLiteral          = 12
	SimplifiedCqlParserRULE_cqlType             = 13
	SimplifiedCqlParserRULE_primitiveType       = 14
	SimplifiedCqlParserRULE_collectionType      = 15
	SimplifiedCqlParserRULE_tupleType           = 16
	SimplifiedCqlParserRULE_functionCall        = 17
	SimplifiedCqlParserRULE_functionArgs        = 18
	SimplifiedCqlParserRULE_tableName           = 19
	SimplifiedCqlParserRULE_functionName        = 20
	SimplifiedCqlParserRULE_userTypeName        = 21
	SimplifiedCqlParserRULE_keyspaceName        = 22
	SimplifiedCqlParserRULE_qualifiedIdentifier = 23
	SimplifiedCqlParserRULE_identifier          = 24
	SimplifiedCqlParserRULE_discardedContent    = 25
	SimplifiedCqlParserRULE_unknown             = 26
)

// ICqlStatementContext is an interface to support dynamic dispatch.
type ICqlStatementContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsCqlStatementContext differentiates from other interfaces.
	IsCqlStatementContext()
}

type CqlStatementContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyCqlStatementContext() *CqlStatementContext {
	var p = new(CqlStatementContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_cqlStatement
	return p
}

func (*CqlStatementContext) IsCqlStatementContext() {}

func NewCqlStatementContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *CqlStatementContext {
	var p = new(CqlStatementContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_cqlStatement

	return p
}

func (s *CqlStatementContext) GetParser() antlr.Parser { return s.parser }

func (s *CqlStatementContext) SelectStatement() ISelectStatementContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*ISelectStatementContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(ISelectStatementContext)
}

func (s *CqlStatementContext) EOS() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserEOS, 0)
}

func (s *CqlStatementContext) OtherStatement() IOtherStatementContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IOtherStatementContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IOtherStatementContext)
}

func (s *CqlStatementContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *CqlStatementContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *CqlStatementContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterCqlStatement(s)
	}
}

func (s *CqlStatementContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitCqlStatement(s)
	}
}

func (p *SimplifiedCqlParser) CqlStatement() (localctx ICqlStatementContext) {
	localctx = NewCqlStatementContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 0, SimplifiedCqlParserRULE_cqlStatement)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.SetState(62)
	p.GetErrorHandler().Sync(p)
	switch p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 2, p.GetParserRuleContext()) {
	case 1:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(54)
			p.SelectStatement()
		}
		p.SetState(56)
		p.GetErrorHandler().Sync(p)
		_la = p.GetTokenStream().LA(1)

		if _la == SimplifiedCqlParserEOS {
			{
				p.SetState(55)
				p.Match(SimplifiedCqlParserEOS)
			}

		}

	case 2:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(58)
			p.OtherStatement()
		}
		p.SetState(60)
		p.GetErrorHandler().Sync(p)
		_la = p.GetTokenStream().LA(1)

		if _la == SimplifiedCqlParserEOS {
			{
				p.SetState(59)
				p.Match(SimplifiedCqlParserEOS)
			}

		}

	}

	return localctx
}

// ISelectStatementContext is an interface to support dynamic dispatch.
type ISelectStatementContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsSelectStatementContext differentiates from other interfaces.
	IsSelectStatementContext()
}

type SelectStatementContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptySelectStatementContext() *SelectStatementContext {
	var p = new(SelectStatementContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_selectStatement
	return p
}

func (*SelectStatementContext) IsSelectStatementContext() {}

func NewSelectStatementContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *SelectStatementContext {
	var p = new(SelectStatementContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_selectStatement

	return p
}

func (s *SelectStatementContext) GetParser() antlr.Parser { return s.parser }

func (s *SelectStatementContext) K_SELECT() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_SELECT, 0)
}

func (s *SelectStatementContext) SelectClause() ISelectClauseContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*ISelectClauseContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(ISelectClauseContext)
}

func (s *SelectStatementContext) K_FROM() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_FROM, 0)
}

func (s *SelectStatementContext) TableName() ITableNameContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*ITableNameContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(ITableNameContext)
}

func (s *SelectStatementContext) DiscardedContent() IDiscardedContentContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IDiscardedContentContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IDiscardedContentContext)
}

func (s *SelectStatementContext) K_JSON() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_JSON, 0)
}

func (s *SelectStatementContext) K_DISTINCT() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_DISTINCT, 0)
}

func (s *SelectStatementContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SelectStatementContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *SelectStatementContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterSelectStatement(s)
	}
}

func (s *SelectStatementContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitSelectStatement(s)
	}
}

func (p *SimplifiedCqlParser) SelectStatement() (localctx ISelectStatementContext) {
	localctx = NewSelectStatementContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 2, SimplifiedCqlParserRULE_selectStatement)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(64)
		p.Match(SimplifiedCqlParserK_SELECT)
	}
	p.SetState(66)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	if _la == SimplifiedCqlParserK_JSON {
		{
			p.SetState(65)
			p.Match(SimplifiedCqlParserK_JSON)
		}

	}
	p.SetState(69)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	if _la == SimplifiedCqlParserK_DISTINCT {
		{
			p.SetState(68)
			p.Match(SimplifiedCqlParserK_DISTINCT)
		}

	}
	{
		p.SetState(71)
		p.SelectClause()
	}
	{
		p.SetState(72)
		p.Match(SimplifiedCqlParserK_FROM)
	}
	{
		p.SetState(73)
		p.TableName()
	}
	{
		p.SetState(74)
		p.DiscardedContent()
	}

	return localctx
}

// IOtherStatementContext is an interface to support dynamic dispatch.
type IOtherStatementContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsOtherStatementContext differentiates from other interfaces.
	IsOtherStatementContext()
}

type OtherStatementContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyOtherStatementContext() *OtherStatementContext {
	var p = new(OtherStatementContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_otherStatement
	return p
}

func (*OtherStatementContext) IsOtherStatementContext() {}

func NewOtherStatementContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *OtherStatementContext {
	var p = new(OtherStatementContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_otherStatement

	return p
}

func (s *OtherStatementContext) GetParser() antlr.Parser { return s.parser }

func (s *OtherStatementContext) DiscardedContent() IDiscardedContentContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IDiscardedContentContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IDiscardedContentContext)
}

func (s *OtherStatementContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *OtherStatementContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *OtherStatementContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterOtherStatement(s)
	}
}

func (s *OtherStatementContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitOtherStatement(s)
	}
}

func (p *SimplifiedCqlParser) OtherStatement() (localctx IOtherStatementContext) {
	localctx = NewOtherStatementContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 4, SimplifiedCqlParserRULE_otherStatement)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(76)
		p.DiscardedContent()
	}

	return localctx
}

// ISelectClauseContext is an interface to support dynamic dispatch.
type ISelectClauseContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsSelectClauseContext differentiates from other interfaces.
	IsSelectClauseContext()
}

type SelectClauseContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptySelectClauseContext() *SelectClauseContext {
	var p = new(SelectClauseContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_selectClause
	return p
}

func (*SelectClauseContext) IsSelectClauseContext() {}

func NewSelectClauseContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *SelectClauseContext {
	var p = new(SelectClauseContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_selectClause

	return p
}

func (s *SelectClauseContext) GetParser() antlr.Parser { return s.parser }

func (s *SelectClauseContext) AllSelector() []ISelectorContext {
	var ts = s.GetTypedRuleContexts(reflect.TypeOf((*ISelectorContext)(nil)).Elem())
	var tst = make([]ISelectorContext, len(ts))

	for i, t := range ts {
		if t != nil {
			tst[i] = t.(ISelectorContext)
		}
	}

	return tst
}

func (s *SelectClauseContext) Selector(i int) ISelectorContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*ISelectorContext)(nil)).Elem(), i)

	if t == nil {
		return nil
	}

	return t.(ISelectorContext)
}

func (s *SelectClauseContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SelectClauseContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *SelectClauseContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterSelectClause(s)
	}
}

func (s *SelectClauseContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitSelectClause(s)
	}
}

func (p *SimplifiedCqlParser) SelectClause() (localctx ISelectClauseContext) {
	localctx = NewSelectClauseContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 6, SimplifiedCqlParserRULE_selectClause)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.SetState(87)
	p.GetErrorHandler().Sync(p)

	switch p.GetTokenStream().LA(1) {
	case SimplifiedCqlParserT__2, SimplifiedCqlParserT__4, SimplifiedCqlParserT__5, SimplifiedCqlParserT__7, SimplifiedCqlParserK_CAST, SimplifiedCqlParserK_INFINITY, SimplifiedCqlParserK_NAN, SimplifiedCqlParserK_NULL, SimplifiedCqlParserSTRING_LITERAL, SimplifiedCqlParserQUOTED_IDENTIFIER, SimplifiedCqlParserINTEGER, SimplifiedCqlParserFLOAT, SimplifiedCqlParserBOOLEAN, SimplifiedCqlParserDURATION, SimplifiedCqlParserUNQUOTED_IDENTIFIER, SimplifiedCqlParserHEXNUMBER, SimplifiedCqlParserUUID:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(78)
			p.Selector()
		}
		p.SetState(83)
		p.GetErrorHandler().Sync(p)
		_la = p.GetTokenStream().LA(1)

		for _la == SimplifiedCqlParserT__0 {
			{
				p.SetState(79)
				p.Match(SimplifiedCqlParserT__0)
			}
			{
				p.SetState(80)
				p.Selector()
			}

			p.SetState(85)
			p.GetErrorHandler().Sync(p)
			_la = p.GetTokenStream().LA(1)
		}

	case SimplifiedCqlParserT__1:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(86)
			p.Match(SimplifiedCqlParserT__1)
		}

	default:
		panic(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
	}

	return localctx
}

// ISelectorContext is an interface to support dynamic dispatch.
type ISelectorContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsSelectorContext differentiates from other interfaces.
	IsSelectorContext()
}

type SelectorContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptySelectorContext() *SelectorContext {
	var p = new(SelectorContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_selector
	return p
}

func (*SelectorContext) IsSelectorContext() {}

func NewSelectorContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *SelectorContext {
	var p = new(SelectorContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_selector

	return p
}

func (s *SelectorContext) GetParser() antlr.Parser { return s.parser }

func (s *SelectorContext) UnaliasedSelector() IUnaliasedSelectorContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IUnaliasedSelectorContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IUnaliasedSelectorContext)
}

func (s *SelectorContext) K_AS() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_AS, 0)
}

func (s *SelectorContext) Identifier() IIdentifierContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IIdentifierContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IIdentifierContext)
}

func (s *SelectorContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SelectorContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *SelectorContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterSelector(s)
	}
}

func (s *SelectorContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitSelector(s)
	}
}

func (p *SimplifiedCqlParser) Selector() (localctx ISelectorContext) {
	localctx = NewSelectorContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 8, SimplifiedCqlParserRULE_selector)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(89)
		p.UnaliasedSelector()
	}
	p.SetState(92)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	if _la == SimplifiedCqlParserK_AS {
		{
			p.SetState(90)
			p.Match(SimplifiedCqlParserK_AS)
		}
		{
			p.SetState(91)
			p.Identifier()
		}

	}

	return localctx
}

// IUnaliasedSelectorContext is an interface to support dynamic dispatch.
type IUnaliasedSelectorContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsUnaliasedSelectorContext differentiates from other interfaces.
	IsUnaliasedSelectorContext()
}

type UnaliasedSelectorContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyUnaliasedSelectorContext() *UnaliasedSelectorContext {
	var p = new(UnaliasedSelectorContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_unaliasedSelector
	return p
}

func (*UnaliasedSelectorContext) IsUnaliasedSelectorContext() {}

func NewUnaliasedSelectorContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *UnaliasedSelectorContext {
	var p = new(UnaliasedSelectorContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_unaliasedSelector

	return p
}

func (s *UnaliasedSelectorContext) GetParser() antlr.Parser { return s.parser }

func (s *UnaliasedSelectorContext) Identifier() IIdentifierContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IIdentifierContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IIdentifierContext)
}

func (s *UnaliasedSelectorContext) Term() ITermContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*ITermContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(ITermContext)
}

func (s *UnaliasedSelectorContext) K_CAST() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_CAST, 0)
}

func (s *UnaliasedSelectorContext) UnaliasedSelector() IUnaliasedSelectorContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IUnaliasedSelectorContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IUnaliasedSelectorContext)
}

func (s *UnaliasedSelectorContext) K_AS() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_AS, 0)
}

func (s *UnaliasedSelectorContext) PrimitiveType() IPrimitiveTypeContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IPrimitiveTypeContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IPrimitiveTypeContext)
}

func (s *UnaliasedSelectorContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *UnaliasedSelectorContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *UnaliasedSelectorContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterUnaliasedSelector(s)
	}
}

func (s *UnaliasedSelectorContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitUnaliasedSelector(s)
	}
}

func (p *SimplifiedCqlParser) UnaliasedSelector() (localctx IUnaliasedSelectorContext) {
	localctx = NewUnaliasedSelectorContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 10, SimplifiedCqlParserRULE_unaliasedSelector)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.SetState(103)
	p.GetErrorHandler().Sync(p)
	switch p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 8, p.GetParserRuleContext()) {
	case 1:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(94)
			p.Identifier()
		}

	case 2:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(95)
			p.Term()
		}

	case 3:
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(96)
			p.Match(SimplifiedCqlParserK_CAST)
		}
		{
			p.SetState(97)
			p.Match(SimplifiedCqlParserT__2)
		}
		{
			p.SetState(98)
			p.UnaliasedSelector()
		}
		{
			p.SetState(99)
			p.Match(SimplifiedCqlParserK_AS)
		}
		{
			p.SetState(100)
			p.PrimitiveType()
		}
		{
			p.SetState(101)
			p.Match(SimplifiedCqlParserT__3)
		}

	}

	return localctx
}

// ITermContext is an interface to support dynamic dispatch.
type ITermContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsTermContext differentiates from other interfaces.
	IsTermContext()
}

type TermContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyTermContext() *TermContext {
	var p = new(TermContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_term
	return p
}

func (*TermContext) IsTermContext() {}

func NewTermContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *TermContext {
	var p = new(TermContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_term

	return p
}

func (s *TermContext) GetParser() antlr.Parser { return s.parser }

func (s *TermContext) Literal() ILiteralContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*ILiteralContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(ILiteralContext)
}

func (s *TermContext) FunctionCall() IFunctionCallContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IFunctionCallContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IFunctionCallContext)
}

func (s *TermContext) CqlType() ICqlTypeContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*ICqlTypeContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(ICqlTypeContext)
}

func (s *TermContext) Term() ITermContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*ITermContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(ITermContext)
}

func (s *TermContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *TermContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *TermContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterTerm(s)
	}
}

func (s *TermContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitTerm(s)
	}
}

func (p *SimplifiedCqlParser) Term() (localctx ITermContext) {
	localctx = NewTermContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 12, SimplifiedCqlParserRULE_term)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.SetState(112)
	p.GetErrorHandler().Sync(p)

	switch p.GetTokenStream().LA(1) {
	case SimplifiedCqlParserT__4, SimplifiedCqlParserT__5, SimplifiedCqlParserT__7, SimplifiedCqlParserK_INFINITY, SimplifiedCqlParserK_NAN, SimplifiedCqlParserK_NULL, SimplifiedCqlParserSTRING_LITERAL, SimplifiedCqlParserINTEGER, SimplifiedCqlParserFLOAT, SimplifiedCqlParserBOOLEAN, SimplifiedCqlParserDURATION, SimplifiedCqlParserHEXNUMBER, SimplifiedCqlParserUUID:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(105)
			p.Literal()
		}

	case SimplifiedCqlParserQUOTED_IDENTIFIER, SimplifiedCqlParserUNQUOTED_IDENTIFIER:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(106)
			p.FunctionCall()
		}

	case SimplifiedCqlParserT__2:
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(107)
			p.Match(SimplifiedCqlParserT__2)
		}
		{
			p.SetState(108)
			p.CqlType()
		}
		{
			p.SetState(109)
			p.Match(SimplifiedCqlParserT__3)
		}
		{
			p.SetState(110)
			p.Term()
		}

	default:
		panic(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
	}

	return localctx
}

// ILiteralContext is an interface to support dynamic dispatch.
type ILiteralContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsLiteralContext differentiates from other interfaces.
	IsLiteralContext()
}

type LiteralContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyLiteralContext() *LiteralContext {
	var p = new(LiteralContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_literal
	return p
}

func (*LiteralContext) IsLiteralContext() {}

func NewLiteralContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *LiteralContext {
	var p = new(LiteralContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_literal

	return p
}

func (s *LiteralContext) GetParser() antlr.Parser { return s.parser }

func (s *LiteralContext) PrimitiveLiteral() IPrimitiveLiteralContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IPrimitiveLiteralContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IPrimitiveLiteralContext)
}

func (s *LiteralContext) CollectionLiteral() ICollectionLiteralContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*ICollectionLiteralContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(ICollectionLiteralContext)
}

func (s *LiteralContext) K_NULL() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_NULL, 0)
}

func (s *LiteralContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *LiteralContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *LiteralContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterLiteral(s)
	}
}

func (s *LiteralContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitLiteral(s)
	}
}

func (p *SimplifiedCqlParser) Literal() (localctx ILiteralContext) {
	localctx = NewLiteralContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 14, SimplifiedCqlParserRULE_literal)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.SetState(117)
	p.GetErrorHandler().Sync(p)

	switch p.GetTokenStream().LA(1) {
	case SimplifiedCqlParserT__4, SimplifiedCqlParserK_INFINITY, SimplifiedCqlParserK_NAN, SimplifiedCqlParserSTRING_LITERAL, SimplifiedCqlParserINTEGER, SimplifiedCqlParserFLOAT, SimplifiedCqlParserBOOLEAN, SimplifiedCqlParserDURATION, SimplifiedCqlParserHEXNUMBER, SimplifiedCqlParserUUID:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(114)
			p.PrimitiveLiteral()
		}

	case SimplifiedCqlParserT__5, SimplifiedCqlParserT__7:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(115)
			p.CollectionLiteral()
		}

	case SimplifiedCqlParserK_NULL:
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(116)
			p.Match(SimplifiedCqlParserK_NULL)
		}

	default:
		panic(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
	}

	return localctx
}

// IPrimitiveLiteralContext is an interface to support dynamic dispatch.
type IPrimitiveLiteralContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsPrimitiveLiteralContext differentiates from other interfaces.
	IsPrimitiveLiteralContext()
}

type PrimitiveLiteralContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyPrimitiveLiteralContext() *PrimitiveLiteralContext {
	var p = new(PrimitiveLiteralContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_primitiveLiteral
	return p
}

func (*PrimitiveLiteralContext) IsPrimitiveLiteralContext() {}

func NewPrimitiveLiteralContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *PrimitiveLiteralContext {
	var p = new(PrimitiveLiteralContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_primitiveLiteral

	return p
}

func (s *PrimitiveLiteralContext) GetParser() antlr.Parser { return s.parser }

func (s *PrimitiveLiteralContext) STRING_LITERAL() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserSTRING_LITERAL, 0)
}

func (s *PrimitiveLiteralContext) INTEGER() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserINTEGER, 0)
}

func (s *PrimitiveLiteralContext) FLOAT() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserFLOAT, 0)
}

func (s *PrimitiveLiteralContext) BOOLEAN() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserBOOLEAN, 0)
}

func (s *PrimitiveLiteralContext) DURATION() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserDURATION, 0)
}

func (s *PrimitiveLiteralContext) UUID() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserUUID, 0)
}

func (s *PrimitiveLiteralContext) HEXNUMBER() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserHEXNUMBER, 0)
}

func (s *PrimitiveLiteralContext) K_NAN() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_NAN, 0)
}

func (s *PrimitiveLiteralContext) K_INFINITY() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_INFINITY, 0)
}

func (s *PrimitiveLiteralContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PrimitiveLiteralContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *PrimitiveLiteralContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterPrimitiveLiteral(s)
	}
}

func (s *PrimitiveLiteralContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitPrimitiveLiteral(s)
	}
}

func (p *SimplifiedCqlParser) PrimitiveLiteral() (localctx IPrimitiveLiteralContext) {
	localctx = NewPrimitiveLiteralContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 16, SimplifiedCqlParserRULE_primitiveLiteral)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.SetState(134)
	p.GetErrorHandler().Sync(p)
	switch p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 13, p.GetParserRuleContext()) {
	case 1:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(119)
			p.Match(SimplifiedCqlParserSTRING_LITERAL)
		}

	case 2:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(120)
			p.Match(SimplifiedCqlParserINTEGER)
		}

	case 3:
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(121)
			p.Match(SimplifiedCqlParserFLOAT)
		}

	case 4:
		p.EnterOuterAlt(localctx, 4)
		{
			p.SetState(122)
			p.Match(SimplifiedCqlParserBOOLEAN)
		}

	case 5:
		p.EnterOuterAlt(localctx, 5)
		{
			p.SetState(123)
			p.Match(SimplifiedCqlParserDURATION)
		}

	case 6:
		p.EnterOuterAlt(localctx, 6)
		{
			p.SetState(124)
			p.Match(SimplifiedCqlParserUUID)
		}

	case 7:
		p.EnterOuterAlt(localctx, 7)
		{
			p.SetState(125)
			p.Match(SimplifiedCqlParserHEXNUMBER)
		}

	case 8:
		p.EnterOuterAlt(localctx, 8)
		p.SetState(127)
		p.GetErrorHandler().Sync(p)
		_la = p.GetTokenStream().LA(1)

		if _la == SimplifiedCqlParserT__4 {
			{
				p.SetState(126)
				p.Match(SimplifiedCqlParserT__4)
			}

		}
		{
			p.SetState(129)
			p.Match(SimplifiedCqlParserK_NAN)
		}

	case 9:
		p.EnterOuterAlt(localctx, 9)
		p.SetState(131)
		p.GetErrorHandler().Sync(p)
		_la = p.GetTokenStream().LA(1)

		if _la == SimplifiedCqlParserT__4 {
			{
				p.SetState(130)
				p.Match(SimplifiedCqlParserT__4)
			}

		}
		{
			p.SetState(133)
			p.Match(SimplifiedCqlParserK_INFINITY)
		}

	}

	return localctx
}

// ICollectionLiteralContext is an interface to support dynamic dispatch.
type ICollectionLiteralContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsCollectionLiteralContext differentiates from other interfaces.
	IsCollectionLiteralContext()
}

type CollectionLiteralContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyCollectionLiteralContext() *CollectionLiteralContext {
	var p = new(CollectionLiteralContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_collectionLiteral
	return p
}

func (*CollectionLiteralContext) IsCollectionLiteralContext() {}

func NewCollectionLiteralContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *CollectionLiteralContext {
	var p = new(CollectionLiteralContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_collectionLiteral

	return p
}

func (s *CollectionLiteralContext) GetParser() antlr.Parser { return s.parser }

func (s *CollectionLiteralContext) ListLiteral() IListLiteralContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IListLiteralContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IListLiteralContext)
}

func (s *CollectionLiteralContext) SetLiteral() ISetLiteralContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*ISetLiteralContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(ISetLiteralContext)
}

func (s *CollectionLiteralContext) MapLiteral() IMapLiteralContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IMapLiteralContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IMapLiteralContext)
}

func (s *CollectionLiteralContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *CollectionLiteralContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *CollectionLiteralContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterCollectionLiteral(s)
	}
}

func (s *CollectionLiteralContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitCollectionLiteral(s)
	}
}

func (p *SimplifiedCqlParser) CollectionLiteral() (localctx ICollectionLiteralContext) {
	localctx = NewCollectionLiteralContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 18, SimplifiedCqlParserRULE_collectionLiteral)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.SetState(139)
	p.GetErrorHandler().Sync(p)
	switch p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 14, p.GetParserRuleContext()) {
	case 1:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(136)
			p.ListLiteral()
		}

	case 2:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(137)
			p.SetLiteral()
		}

	case 3:
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(138)
			p.MapLiteral()
		}

	}

	return localctx
}

// IListLiteralContext is an interface to support dynamic dispatch.
type IListLiteralContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsListLiteralContext differentiates from other interfaces.
	IsListLiteralContext()
}

type ListLiteralContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyListLiteralContext() *ListLiteralContext {
	var p = new(ListLiteralContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_listLiteral
	return p
}

func (*ListLiteralContext) IsListLiteralContext() {}

func NewListLiteralContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *ListLiteralContext {
	var p = new(ListLiteralContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_listLiteral

	return p
}

func (s *ListLiteralContext) GetParser() antlr.Parser { return s.parser }

func (s *ListLiteralContext) AllTerm() []ITermContext {
	var ts = s.GetTypedRuleContexts(reflect.TypeOf((*ITermContext)(nil)).Elem())
	var tst = make([]ITermContext, len(ts))

	for i, t := range ts {
		if t != nil {
			tst[i] = t.(ITermContext)
		}
	}

	return tst
}

func (s *ListLiteralContext) Term(i int) ITermContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*ITermContext)(nil)).Elem(), i)

	if t == nil {
		return nil
	}

	return t.(ITermContext)
}

func (s *ListLiteralContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ListLiteralContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *ListLiteralContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterListLiteral(s)
	}
}

func (s *ListLiteralContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitListLiteral(s)
	}
}

func (p *SimplifiedCqlParser) ListLiteral() (localctx IListLiteralContext) {
	localctx = NewListLiteralContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 20, SimplifiedCqlParserRULE_listLiteral)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(141)
		p.Match(SimplifiedCqlParserT__5)
	}
	p.SetState(150)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	if (((_la)&-(0x1f+1)) == 0 && ((1<<uint(_la))&((1<<SimplifiedCqlParserT__2)|(1<<SimplifiedCqlParserT__4)|(1<<SimplifiedCqlParserT__5)|(1<<SimplifiedCqlParserT__7)|(1<<SimplifiedCqlParserK_INFINITY))) != 0) || (((_la-35)&-(0x1f+1)) == 0 && ((1<<uint((_la-35)))&((1<<(SimplifiedCqlParserK_NAN-35))|(1<<(SimplifiedCqlParserK_NULL-35))|(1<<(SimplifiedCqlParserSTRING_LITERAL-35))|(1<<(SimplifiedCqlParserQUOTED_IDENTIFIER-35))|(1<<(SimplifiedCqlParserINTEGER-35))|(1<<(SimplifiedCqlParserFLOAT-35))|(1<<(SimplifiedCqlParserBOOLEAN-35))|(1<<(SimplifiedCqlParserDURATION-35))|(1<<(SimplifiedCqlParserUNQUOTED_IDENTIFIER-35))|(1<<(SimplifiedCqlParserHEXNUMBER-35))|(1<<(SimplifiedCqlParserUUID-35)))) != 0) {
		{
			p.SetState(142)
			p.Term()
		}
		p.SetState(147)
		p.GetErrorHandler().Sync(p)
		_la = p.GetTokenStream().LA(1)

		for _la == SimplifiedCqlParserT__0 {
			{
				p.SetState(143)
				p.Match(SimplifiedCqlParserT__0)
			}
			{
				p.SetState(144)
				p.Term()
			}

			p.SetState(149)
			p.GetErrorHandler().Sync(p)
			_la = p.GetTokenStream().LA(1)
		}

	}
	{
		p.SetState(152)
		p.Match(SimplifiedCqlParserT__6)
	}

	return localctx
}

// ISetLiteralContext is an interface to support dynamic dispatch.
type ISetLiteralContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsSetLiteralContext differentiates from other interfaces.
	IsSetLiteralContext()
}

type SetLiteralContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptySetLiteralContext() *SetLiteralContext {
	var p = new(SetLiteralContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_setLiteral
	return p
}

func (*SetLiteralContext) IsSetLiteralContext() {}

func NewSetLiteralContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *SetLiteralContext {
	var p = new(SetLiteralContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_setLiteral

	return p
}

func (s *SetLiteralContext) GetParser() antlr.Parser { return s.parser }

func (s *SetLiteralContext) AllTerm() []ITermContext {
	var ts = s.GetTypedRuleContexts(reflect.TypeOf((*ITermContext)(nil)).Elem())
	var tst = make([]ITermContext, len(ts))

	for i, t := range ts {
		if t != nil {
			tst[i] = t.(ITermContext)
		}
	}

	return tst
}

func (s *SetLiteralContext) Term(i int) ITermContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*ITermContext)(nil)).Elem(), i)

	if t == nil {
		return nil
	}

	return t.(ITermContext)
}

func (s *SetLiteralContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SetLiteralContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *SetLiteralContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterSetLiteral(s)
	}
}

func (s *SetLiteralContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitSetLiteral(s)
	}
}

func (p *SimplifiedCqlParser) SetLiteral() (localctx ISetLiteralContext) {
	localctx = NewSetLiteralContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 22, SimplifiedCqlParserRULE_setLiteral)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(154)
		p.Match(SimplifiedCqlParserT__7)
	}
	p.SetState(163)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	if (((_la)&-(0x1f+1)) == 0 && ((1<<uint(_la))&((1<<SimplifiedCqlParserT__2)|(1<<SimplifiedCqlParserT__4)|(1<<SimplifiedCqlParserT__5)|(1<<SimplifiedCqlParserT__7)|(1<<SimplifiedCqlParserK_INFINITY))) != 0) || (((_la-35)&-(0x1f+1)) == 0 && ((1<<uint((_la-35)))&((1<<(SimplifiedCqlParserK_NAN-35))|(1<<(SimplifiedCqlParserK_NULL-35))|(1<<(SimplifiedCqlParserSTRING_LITERAL-35))|(1<<(SimplifiedCqlParserQUOTED_IDENTIFIER-35))|(1<<(SimplifiedCqlParserINTEGER-35))|(1<<(SimplifiedCqlParserFLOAT-35))|(1<<(SimplifiedCqlParserBOOLEAN-35))|(1<<(SimplifiedCqlParserDURATION-35))|(1<<(SimplifiedCqlParserUNQUOTED_IDENTIFIER-35))|(1<<(SimplifiedCqlParserHEXNUMBER-35))|(1<<(SimplifiedCqlParserUUID-35)))) != 0) {
		{
			p.SetState(155)
			p.Term()
		}
		p.SetState(160)
		p.GetErrorHandler().Sync(p)
		_la = p.GetTokenStream().LA(1)

		for _la == SimplifiedCqlParserT__0 {
			{
				p.SetState(156)
				p.Match(SimplifiedCqlParserT__0)
			}
			{
				p.SetState(157)
				p.Term()
			}

			p.SetState(162)
			p.GetErrorHandler().Sync(p)
			_la = p.GetTokenStream().LA(1)
		}

	}
	{
		p.SetState(165)
		p.Match(SimplifiedCqlParserT__8)
	}

	return localctx
}

// IMapLiteralContext is an interface to support dynamic dispatch.
type IMapLiteralContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsMapLiteralContext differentiates from other interfaces.
	IsMapLiteralContext()
}

type MapLiteralContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyMapLiteralContext() *MapLiteralContext {
	var p = new(MapLiteralContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_mapLiteral
	return p
}

func (*MapLiteralContext) IsMapLiteralContext() {}

func NewMapLiteralContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *MapLiteralContext {
	var p = new(MapLiteralContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_mapLiteral

	return p
}

func (s *MapLiteralContext) GetParser() antlr.Parser { return s.parser }

func (s *MapLiteralContext) AllTerm() []ITermContext {
	var ts = s.GetTypedRuleContexts(reflect.TypeOf((*ITermContext)(nil)).Elem())
	var tst = make([]ITermContext, len(ts))

	for i, t := range ts {
		if t != nil {
			tst[i] = t.(ITermContext)
		}
	}

	return tst
}

func (s *MapLiteralContext) Term(i int) ITermContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*ITermContext)(nil)).Elem(), i)

	if t == nil {
		return nil
	}

	return t.(ITermContext)
}

func (s *MapLiteralContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *MapLiteralContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *MapLiteralContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterMapLiteral(s)
	}
}

func (s *MapLiteralContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitMapLiteral(s)
	}
}

func (p *SimplifiedCqlParser) MapLiteral() (localctx IMapLiteralContext) {
	localctx = NewMapLiteralContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 24, SimplifiedCqlParserRULE_mapLiteral)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(167)
		p.Match(SimplifiedCqlParserT__7)
	}
	p.SetState(181)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	if (((_la)&-(0x1f+1)) == 0 && ((1<<uint(_la))&((1<<SimplifiedCqlParserT__2)|(1<<SimplifiedCqlParserT__4)|(1<<SimplifiedCqlParserT__5)|(1<<SimplifiedCqlParserT__7)|(1<<SimplifiedCqlParserK_INFINITY))) != 0) || (((_la-35)&-(0x1f+1)) == 0 && ((1<<uint((_la-35)))&((1<<(SimplifiedCqlParserK_NAN-35))|(1<<(SimplifiedCqlParserK_NULL-35))|(1<<(SimplifiedCqlParserSTRING_LITERAL-35))|(1<<(SimplifiedCqlParserQUOTED_IDENTIFIER-35))|(1<<(SimplifiedCqlParserINTEGER-35))|(1<<(SimplifiedCqlParserFLOAT-35))|(1<<(SimplifiedCqlParserBOOLEAN-35))|(1<<(SimplifiedCqlParserDURATION-35))|(1<<(SimplifiedCqlParserUNQUOTED_IDENTIFIER-35))|(1<<(SimplifiedCqlParserHEXNUMBER-35))|(1<<(SimplifiedCqlParserUUID-35)))) != 0) {
		{
			p.SetState(168)
			p.Term()
		}
		{
			p.SetState(169)
			p.Match(SimplifiedCqlParserT__9)
		}
		{
			p.SetState(170)
			p.Term()
		}
		p.SetState(178)
		p.GetErrorHandler().Sync(p)
		_la = p.GetTokenStream().LA(1)

		for _la == SimplifiedCqlParserT__0 {
			{
				p.SetState(171)
				p.Match(SimplifiedCqlParserT__0)
			}
			{
				p.SetState(172)
				p.Term()
			}
			{
				p.SetState(173)
				p.Match(SimplifiedCqlParserT__9)
			}
			{
				p.SetState(174)
				p.Term()
			}

			p.SetState(180)
			p.GetErrorHandler().Sync(p)
			_la = p.GetTokenStream().LA(1)
		}

	}
	{
		p.SetState(183)
		p.Match(SimplifiedCqlParserT__8)
	}

	return localctx
}

// ICqlTypeContext is an interface to support dynamic dispatch.
type ICqlTypeContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsCqlTypeContext differentiates from other interfaces.
	IsCqlTypeContext()
}

type CqlTypeContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyCqlTypeContext() *CqlTypeContext {
	var p = new(CqlTypeContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_cqlType
	return p
}

func (*CqlTypeContext) IsCqlTypeContext() {}

func NewCqlTypeContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *CqlTypeContext {
	var p = new(CqlTypeContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_cqlType

	return p
}

func (s *CqlTypeContext) GetParser() antlr.Parser { return s.parser }

func (s *CqlTypeContext) PrimitiveType() IPrimitiveTypeContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IPrimitiveTypeContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IPrimitiveTypeContext)
}

func (s *CqlTypeContext) CollectionType() ICollectionTypeContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*ICollectionTypeContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(ICollectionTypeContext)
}

func (s *CqlTypeContext) TupleType() ITupleTypeContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*ITupleTypeContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(ITupleTypeContext)
}

func (s *CqlTypeContext) UserTypeName() IUserTypeNameContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IUserTypeNameContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IUserTypeNameContext)
}

func (s *CqlTypeContext) K_FROZEN() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_FROZEN, 0)
}

func (s *CqlTypeContext) CqlType() ICqlTypeContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*ICqlTypeContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(ICqlTypeContext)
}

func (s *CqlTypeContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *CqlTypeContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *CqlTypeContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterCqlType(s)
	}
}

func (s *CqlTypeContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitCqlType(s)
	}
}

func (p *SimplifiedCqlParser) CqlType() (localctx ICqlTypeContext) {
	localctx = NewCqlTypeContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 26, SimplifiedCqlParserRULE_cqlType)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.SetState(194)
	p.GetErrorHandler().Sync(p)

	switch p.GetTokenStream().LA(1) {
	case SimplifiedCqlParserK_ASCII, SimplifiedCqlParserK_BIGINT, SimplifiedCqlParserK_BLOB, SimplifiedCqlParserK_BOOLEAN, SimplifiedCqlParserK_COUNTER, SimplifiedCqlParserK_DATE, SimplifiedCqlParserK_DECIMAL, SimplifiedCqlParserK_DOUBLE, SimplifiedCqlParserK_DURATION, SimplifiedCqlParserK_FLOAT, SimplifiedCqlParserK_INET, SimplifiedCqlParserK_INT, SimplifiedCqlParserK_SMALLINT, SimplifiedCqlParserK_TEXT, SimplifiedCqlParserK_TIME, SimplifiedCqlParserK_TIMESTAMP, SimplifiedCqlParserK_TIMEUUID, SimplifiedCqlParserK_TINYINT, SimplifiedCqlParserK_UUID, SimplifiedCqlParserK_VARCHAR, SimplifiedCqlParserK_VARINT:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(185)
			p.PrimitiveType()
		}

	case SimplifiedCqlParserK_LIST, SimplifiedCqlParserK_MAP, SimplifiedCqlParserK_SET:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(186)
			p.CollectionType()
		}

	case SimplifiedCqlParserK_TUPLE:
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(187)
			p.TupleType()
		}

	case SimplifiedCqlParserQUOTED_IDENTIFIER, SimplifiedCqlParserUNQUOTED_IDENTIFIER:
		p.EnterOuterAlt(localctx, 4)
		{
			p.SetState(188)
			p.UserTypeName()
		}

	case SimplifiedCqlParserK_FROZEN:
		p.EnterOuterAlt(localctx, 5)
		{
			p.SetState(189)
			p.Match(SimplifiedCqlParserK_FROZEN)
		}
		{
			p.SetState(190)
			p.Match(SimplifiedCqlParserT__10)
		}
		{
			p.SetState(191)
			p.CqlType()
		}
		{
			p.SetState(192)
			p.Match(SimplifiedCqlParserT__11)
		}

	default:
		panic(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
	}

	return localctx
}

// IPrimitiveTypeContext is an interface to support dynamic dispatch.
type IPrimitiveTypeContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsPrimitiveTypeContext differentiates from other interfaces.
	IsPrimitiveTypeContext()
}

type PrimitiveTypeContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyPrimitiveTypeContext() *PrimitiveTypeContext {
	var p = new(PrimitiveTypeContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_primitiveType
	return p
}

func (*PrimitiveTypeContext) IsPrimitiveTypeContext() {}

func NewPrimitiveTypeContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *PrimitiveTypeContext {
	var p = new(PrimitiveTypeContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_primitiveType

	return p
}

func (s *PrimitiveTypeContext) GetParser() antlr.Parser { return s.parser }

func (s *PrimitiveTypeContext) K_ASCII() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_ASCII, 0)
}

func (s *PrimitiveTypeContext) K_BIGINT() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_BIGINT, 0)
}

func (s *PrimitiveTypeContext) K_BLOB() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_BLOB, 0)
}

func (s *PrimitiveTypeContext) K_BOOLEAN() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_BOOLEAN, 0)
}

func (s *PrimitiveTypeContext) K_COUNTER() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_COUNTER, 0)
}

func (s *PrimitiveTypeContext) K_DATE() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_DATE, 0)
}

func (s *PrimitiveTypeContext) K_DECIMAL() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_DECIMAL, 0)
}

func (s *PrimitiveTypeContext) K_DOUBLE() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_DOUBLE, 0)
}

func (s *PrimitiveTypeContext) K_DURATION() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_DURATION, 0)
}

func (s *PrimitiveTypeContext) K_FLOAT() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_FLOAT, 0)
}

func (s *PrimitiveTypeContext) K_INET() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_INET, 0)
}

func (s *PrimitiveTypeContext) K_INT() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_INT, 0)
}

func (s *PrimitiveTypeContext) K_SMALLINT() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_SMALLINT, 0)
}

func (s *PrimitiveTypeContext) K_TEXT() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_TEXT, 0)
}

func (s *PrimitiveTypeContext) K_TIME() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_TIME, 0)
}

func (s *PrimitiveTypeContext) K_TIMESTAMP() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_TIMESTAMP, 0)
}

func (s *PrimitiveTypeContext) K_TIMEUUID() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_TIMEUUID, 0)
}

func (s *PrimitiveTypeContext) K_TINYINT() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_TINYINT, 0)
}

func (s *PrimitiveTypeContext) K_UUID() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_UUID, 0)
}

func (s *PrimitiveTypeContext) K_VARCHAR() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_VARCHAR, 0)
}

func (s *PrimitiveTypeContext) K_VARINT() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_VARINT, 0)
}

func (s *PrimitiveTypeContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PrimitiveTypeContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *PrimitiveTypeContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterPrimitiveType(s)
	}
}

func (s *PrimitiveTypeContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitPrimitiveType(s)
	}
}

func (p *SimplifiedCqlParser) PrimitiveType() (localctx IPrimitiveTypeContext) {
	localctx = NewPrimitiveTypeContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 28, SimplifiedCqlParserRULE_primitiveType)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(196)
		_la = p.GetTokenStream().LA(1)

		if !((((_la)&-(0x1f+1)) == 0 && ((1<<uint(_la))&((1<<SimplifiedCqlParserK_ASCII)|(1<<SimplifiedCqlParserK_BIGINT)|(1<<SimplifiedCqlParserK_BLOB)|(1<<SimplifiedCqlParserK_BOOLEAN)|(1<<SimplifiedCqlParserK_COUNTER)|(1<<SimplifiedCqlParserK_DATE)|(1<<SimplifiedCqlParserK_DECIMAL)|(1<<SimplifiedCqlParserK_DOUBLE)|(1<<SimplifiedCqlParserK_DURATION)|(1<<SimplifiedCqlParserK_FLOAT)|(1<<SimplifiedCqlParserK_INET)|(1<<SimplifiedCqlParserK_INT))) != 0) || (((_la-39)&-(0x1f+1)) == 0 && ((1<<uint((_la-39)))&((1<<(SimplifiedCqlParserK_SMALLINT-39))|(1<<(SimplifiedCqlParserK_TEXT-39))|(1<<(SimplifiedCqlParserK_TIME-39))|(1<<(SimplifiedCqlParserK_TIMESTAMP-39))|(1<<(SimplifiedCqlParserK_TIMEUUID-39))|(1<<(SimplifiedCqlParserK_TINYINT-39))|(1<<(SimplifiedCqlParserK_UUID-39))|(1<<(SimplifiedCqlParserK_VARCHAR-39))|(1<<(SimplifiedCqlParserK_VARINT-39)))) != 0)) {
			p.GetErrorHandler().RecoverInline(p)
		} else {
			p.GetErrorHandler().ReportMatch(p)
			p.Consume()
		}
	}

	return localctx
}

// ICollectionTypeContext is an interface to support dynamic dispatch.
type ICollectionTypeContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsCollectionTypeContext differentiates from other interfaces.
	IsCollectionTypeContext()
}

type CollectionTypeContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyCollectionTypeContext() *CollectionTypeContext {
	var p = new(CollectionTypeContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_collectionType
	return p
}

func (*CollectionTypeContext) IsCollectionTypeContext() {}

func NewCollectionTypeContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *CollectionTypeContext {
	var p = new(CollectionTypeContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_collectionType

	return p
}

func (s *CollectionTypeContext) GetParser() antlr.Parser { return s.parser }

func (s *CollectionTypeContext) K_LIST() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_LIST, 0)
}

func (s *CollectionTypeContext) AllCqlType() []ICqlTypeContext {
	var ts = s.GetTypedRuleContexts(reflect.TypeOf((*ICqlTypeContext)(nil)).Elem())
	var tst = make([]ICqlTypeContext, len(ts))

	for i, t := range ts {
		if t != nil {
			tst[i] = t.(ICqlTypeContext)
		}
	}

	return tst
}

func (s *CollectionTypeContext) CqlType(i int) ICqlTypeContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*ICqlTypeContext)(nil)).Elem(), i)

	if t == nil {
		return nil
	}

	return t.(ICqlTypeContext)
}

func (s *CollectionTypeContext) K_SET() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_SET, 0)
}

func (s *CollectionTypeContext) K_MAP() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_MAP, 0)
}

func (s *CollectionTypeContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *CollectionTypeContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *CollectionTypeContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterCollectionType(s)
	}
}

func (s *CollectionTypeContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitCollectionType(s)
	}
}

func (p *SimplifiedCqlParser) CollectionType() (localctx ICollectionTypeContext) {
	localctx = NewCollectionTypeContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 30, SimplifiedCqlParserRULE_collectionType)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.SetState(215)
	p.GetErrorHandler().Sync(p)

	switch p.GetTokenStream().LA(1) {
	case SimplifiedCqlParserK_LIST:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(198)
			p.Match(SimplifiedCqlParserK_LIST)
		}
		{
			p.SetState(199)
			p.Match(SimplifiedCqlParserT__10)
		}
		{
			p.SetState(200)
			p.CqlType()
		}
		{
			p.SetState(201)
			p.Match(SimplifiedCqlParserT__11)
		}

	case SimplifiedCqlParserK_SET:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(203)
			p.Match(SimplifiedCqlParserK_SET)
		}
		{
			p.SetState(204)
			p.Match(SimplifiedCqlParserT__10)
		}
		{
			p.SetState(205)
			p.CqlType()
		}
		{
			p.SetState(206)
			p.Match(SimplifiedCqlParserT__11)
		}

	case SimplifiedCqlParserK_MAP:
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(208)
			p.Match(SimplifiedCqlParserK_MAP)
		}
		{
			p.SetState(209)
			p.Match(SimplifiedCqlParserT__10)
		}
		{
			p.SetState(210)
			p.CqlType()
		}
		{
			p.SetState(211)
			p.Match(SimplifiedCqlParserT__0)
		}
		{
			p.SetState(212)
			p.CqlType()
		}
		{
			p.SetState(213)
			p.Match(SimplifiedCqlParserT__11)
		}

	default:
		panic(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
	}

	return localctx
}

// ITupleTypeContext is an interface to support dynamic dispatch.
type ITupleTypeContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsTupleTypeContext differentiates from other interfaces.
	IsTupleTypeContext()
}

type TupleTypeContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyTupleTypeContext() *TupleTypeContext {
	var p = new(TupleTypeContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_tupleType
	return p
}

func (*TupleTypeContext) IsTupleTypeContext() {}

func NewTupleTypeContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *TupleTypeContext {
	var p = new(TupleTypeContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_tupleType

	return p
}

func (s *TupleTypeContext) GetParser() antlr.Parser { return s.parser }

func (s *TupleTypeContext) K_TUPLE() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserK_TUPLE, 0)
}

func (s *TupleTypeContext) AllCqlType() []ICqlTypeContext {
	var ts = s.GetTypedRuleContexts(reflect.TypeOf((*ICqlTypeContext)(nil)).Elem())
	var tst = make([]ICqlTypeContext, len(ts))

	for i, t := range ts {
		if t != nil {
			tst[i] = t.(ICqlTypeContext)
		}
	}

	return tst
}

func (s *TupleTypeContext) CqlType(i int) ICqlTypeContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*ICqlTypeContext)(nil)).Elem(), i)

	if t == nil {
		return nil
	}

	return t.(ICqlTypeContext)
}

func (s *TupleTypeContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *TupleTypeContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *TupleTypeContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterTupleType(s)
	}
}

func (s *TupleTypeContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitTupleType(s)
	}
}

func (p *SimplifiedCqlParser) TupleType() (localctx ITupleTypeContext) {
	localctx = NewTupleTypeContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 32, SimplifiedCqlParserRULE_tupleType)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(217)
		p.Match(SimplifiedCqlParserK_TUPLE)
	}
	{
		p.SetState(218)
		p.Match(SimplifiedCqlParserT__10)
	}
	{
		p.SetState(219)
		p.CqlType()
	}
	p.SetState(224)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	for _la == SimplifiedCqlParserT__0 {
		{
			p.SetState(220)
			p.Match(SimplifiedCqlParserT__0)
		}
		{
			p.SetState(221)
			p.CqlType()
		}

		p.SetState(226)
		p.GetErrorHandler().Sync(p)
		_la = p.GetTokenStream().LA(1)
	}
	{
		p.SetState(227)
		p.Match(SimplifiedCqlParserT__11)
	}

	return localctx
}

// IFunctionCallContext is an interface to support dynamic dispatch.
type IFunctionCallContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsFunctionCallContext differentiates from other interfaces.
	IsFunctionCallContext()
}

type FunctionCallContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyFunctionCallContext() *FunctionCallContext {
	var p = new(FunctionCallContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_functionCall
	return p
}

func (*FunctionCallContext) IsFunctionCallContext() {}

func NewFunctionCallContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *FunctionCallContext {
	var p = new(FunctionCallContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_functionCall

	return p
}

func (s *FunctionCallContext) GetParser() antlr.Parser { return s.parser }

func (s *FunctionCallContext) FunctionName() IFunctionNameContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IFunctionNameContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IFunctionNameContext)
}

func (s *FunctionCallContext) FunctionArgs() IFunctionArgsContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IFunctionArgsContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IFunctionArgsContext)
}

func (s *FunctionCallContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *FunctionCallContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *FunctionCallContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterFunctionCall(s)
	}
}

func (s *FunctionCallContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitFunctionCall(s)
	}
}

func (p *SimplifiedCqlParser) FunctionCall() (localctx IFunctionCallContext) {
	localctx = NewFunctionCallContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 34, SimplifiedCqlParserRULE_functionCall)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.SetState(243)
	p.GetErrorHandler().Sync(p)
	switch p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 24, p.GetParserRuleContext()) {
	case 1:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(229)
			p.FunctionName()
		}
		{
			p.SetState(230)
			p.Match(SimplifiedCqlParserT__2)
		}
		{
			p.SetState(231)
			p.Match(SimplifiedCqlParserT__3)
		}

	case 2:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(233)
			p.FunctionName()
		}
		{
			p.SetState(234)
			p.Match(SimplifiedCqlParserT__2)
		}
		{
			p.SetState(235)
			p.Match(SimplifiedCqlParserT__1)
		}
		{
			p.SetState(236)
			p.Match(SimplifiedCqlParserT__3)
		}

	case 3:
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(238)
			p.FunctionName()
		}
		{
			p.SetState(239)
			p.Match(SimplifiedCqlParserT__2)
		}
		{
			p.SetState(240)
			p.FunctionArgs()
		}
		{
			p.SetState(241)
			p.Match(SimplifiedCqlParserT__3)
		}

	}

	return localctx
}

// IFunctionArgsContext is an interface to support dynamic dispatch.
type IFunctionArgsContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsFunctionArgsContext differentiates from other interfaces.
	IsFunctionArgsContext()
}

type FunctionArgsContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyFunctionArgsContext() *FunctionArgsContext {
	var p = new(FunctionArgsContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_functionArgs
	return p
}

func (*FunctionArgsContext) IsFunctionArgsContext() {}

func NewFunctionArgsContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *FunctionArgsContext {
	var p = new(FunctionArgsContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_functionArgs

	return p
}

func (s *FunctionArgsContext) GetParser() antlr.Parser { return s.parser }

func (s *FunctionArgsContext) AllUnaliasedSelector() []IUnaliasedSelectorContext {
	var ts = s.GetTypedRuleContexts(reflect.TypeOf((*IUnaliasedSelectorContext)(nil)).Elem())
	var tst = make([]IUnaliasedSelectorContext, len(ts))

	for i, t := range ts {
		if t != nil {
			tst[i] = t.(IUnaliasedSelectorContext)
		}
	}

	return tst
}

func (s *FunctionArgsContext) UnaliasedSelector(i int) IUnaliasedSelectorContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IUnaliasedSelectorContext)(nil)).Elem(), i)

	if t == nil {
		return nil
	}

	return t.(IUnaliasedSelectorContext)
}

func (s *FunctionArgsContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *FunctionArgsContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *FunctionArgsContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterFunctionArgs(s)
	}
}

func (s *FunctionArgsContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitFunctionArgs(s)
	}
}

func (p *SimplifiedCqlParser) FunctionArgs() (localctx IFunctionArgsContext) {
	localctx = NewFunctionArgsContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 36, SimplifiedCqlParserRULE_functionArgs)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(245)
		p.UnaliasedSelector()
	}
	p.SetState(250)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	for _la == SimplifiedCqlParserT__0 {
		{
			p.SetState(246)
			p.Match(SimplifiedCqlParserT__0)
		}
		{
			p.SetState(247)
			p.UnaliasedSelector()
		}

		p.SetState(252)
		p.GetErrorHandler().Sync(p)
		_la = p.GetTokenStream().LA(1)
	}

	return localctx
}

// ITableNameContext is an interface to support dynamic dispatch.
type ITableNameContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsTableNameContext differentiates from other interfaces.
	IsTableNameContext()
}

type TableNameContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyTableNameContext() *TableNameContext {
	var p = new(TableNameContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_tableName
	return p
}

func (*TableNameContext) IsTableNameContext() {}

func NewTableNameContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *TableNameContext {
	var p = new(TableNameContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_tableName

	return p
}

func (s *TableNameContext) GetParser() antlr.Parser { return s.parser }

func (s *TableNameContext) QualifiedIdentifier() IQualifiedIdentifierContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IQualifiedIdentifierContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IQualifiedIdentifierContext)
}

func (s *TableNameContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *TableNameContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *TableNameContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterTableName(s)
	}
}

func (s *TableNameContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitTableName(s)
	}
}

func (p *SimplifiedCqlParser) TableName() (localctx ITableNameContext) {
	localctx = NewTableNameContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 38, SimplifiedCqlParserRULE_tableName)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(253)
		p.QualifiedIdentifier()
	}

	return localctx
}

// IFunctionNameContext is an interface to support dynamic dispatch.
type IFunctionNameContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsFunctionNameContext differentiates from other interfaces.
	IsFunctionNameContext()
}

type FunctionNameContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyFunctionNameContext() *FunctionNameContext {
	var p = new(FunctionNameContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_functionName
	return p
}

func (*FunctionNameContext) IsFunctionNameContext() {}

func NewFunctionNameContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *FunctionNameContext {
	var p = new(FunctionNameContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_functionName

	return p
}

func (s *FunctionNameContext) GetParser() antlr.Parser { return s.parser }

func (s *FunctionNameContext) QualifiedIdentifier() IQualifiedIdentifierContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IQualifiedIdentifierContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IQualifiedIdentifierContext)
}

func (s *FunctionNameContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *FunctionNameContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *FunctionNameContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterFunctionName(s)
	}
}

func (s *FunctionNameContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitFunctionName(s)
	}
}

func (p *SimplifiedCqlParser) FunctionName() (localctx IFunctionNameContext) {
	localctx = NewFunctionNameContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 40, SimplifiedCqlParserRULE_functionName)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(255)
		p.QualifiedIdentifier()
	}

	return localctx
}

// IUserTypeNameContext is an interface to support dynamic dispatch.
type IUserTypeNameContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsUserTypeNameContext differentiates from other interfaces.
	IsUserTypeNameContext()
}

type UserTypeNameContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyUserTypeNameContext() *UserTypeNameContext {
	var p = new(UserTypeNameContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_userTypeName
	return p
}

func (*UserTypeNameContext) IsUserTypeNameContext() {}

func NewUserTypeNameContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *UserTypeNameContext {
	var p = new(UserTypeNameContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_userTypeName

	return p
}

func (s *UserTypeNameContext) GetParser() antlr.Parser { return s.parser }

func (s *UserTypeNameContext) QualifiedIdentifier() IQualifiedIdentifierContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IQualifiedIdentifierContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IQualifiedIdentifierContext)
}

func (s *UserTypeNameContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *UserTypeNameContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *UserTypeNameContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterUserTypeName(s)
	}
}

func (s *UserTypeNameContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitUserTypeName(s)
	}
}

func (p *SimplifiedCqlParser) UserTypeName() (localctx IUserTypeNameContext) {
	localctx = NewUserTypeNameContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 42, SimplifiedCqlParserRULE_userTypeName)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(257)
		p.QualifiedIdentifier()
	}

	return localctx
}

// IKeyspaceNameContext is an interface to support dynamic dispatch.
type IKeyspaceNameContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsKeyspaceNameContext differentiates from other interfaces.
	IsKeyspaceNameContext()
}

type KeyspaceNameContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyKeyspaceNameContext() *KeyspaceNameContext {
	var p = new(KeyspaceNameContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_keyspaceName
	return p
}

func (*KeyspaceNameContext) IsKeyspaceNameContext() {}

func NewKeyspaceNameContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *KeyspaceNameContext {
	var p = new(KeyspaceNameContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_keyspaceName

	return p
}

func (s *KeyspaceNameContext) GetParser() antlr.Parser { return s.parser }

func (s *KeyspaceNameContext) Identifier() IIdentifierContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IIdentifierContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IIdentifierContext)
}

func (s *KeyspaceNameContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *KeyspaceNameContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *KeyspaceNameContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterKeyspaceName(s)
	}
}

func (s *KeyspaceNameContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitKeyspaceName(s)
	}
}

func (p *SimplifiedCqlParser) KeyspaceName() (localctx IKeyspaceNameContext) {
	localctx = NewKeyspaceNameContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 44, SimplifiedCqlParserRULE_keyspaceName)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(259)
		p.Identifier()
	}

	return localctx
}

// IQualifiedIdentifierContext is an interface to support dynamic dispatch.
type IQualifiedIdentifierContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsQualifiedIdentifierContext differentiates from other interfaces.
	IsQualifiedIdentifierContext()
}

type QualifiedIdentifierContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyQualifiedIdentifierContext() *QualifiedIdentifierContext {
	var p = new(QualifiedIdentifierContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_qualifiedIdentifier
	return p
}

func (*QualifiedIdentifierContext) IsQualifiedIdentifierContext() {}

func NewQualifiedIdentifierContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *QualifiedIdentifierContext {
	var p = new(QualifiedIdentifierContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_qualifiedIdentifier

	return p
}

func (s *QualifiedIdentifierContext) GetParser() antlr.Parser { return s.parser }

func (s *QualifiedIdentifierContext) Identifier() IIdentifierContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IIdentifierContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IIdentifierContext)
}

func (s *QualifiedIdentifierContext) KeyspaceName() IKeyspaceNameContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IKeyspaceNameContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IKeyspaceNameContext)
}

func (s *QualifiedIdentifierContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *QualifiedIdentifierContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *QualifiedIdentifierContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterQualifiedIdentifier(s)
	}
}

func (s *QualifiedIdentifierContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitQualifiedIdentifier(s)
	}
}

func (p *SimplifiedCqlParser) QualifiedIdentifier() (localctx IQualifiedIdentifierContext) {
	localctx = NewQualifiedIdentifierContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 46, SimplifiedCqlParserRULE_qualifiedIdentifier)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	p.SetState(264)
	p.GetErrorHandler().Sync(p)

	if p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 26, p.GetParserRuleContext()) == 1 {
		{
			p.SetState(261)
			p.KeyspaceName()
		}
		{
			p.SetState(262)
			p.Match(SimplifiedCqlParserT__12)
		}

	}
	{
		p.SetState(266)
		p.Identifier()
	}

	return localctx
}

// IIdentifierContext is an interface to support dynamic dispatch.
type IIdentifierContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsIdentifierContext differentiates from other interfaces.
	IsIdentifierContext()
}

type IdentifierContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyIdentifierContext() *IdentifierContext {
	var p = new(IdentifierContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_identifier
	return p
}

func (*IdentifierContext) IsIdentifierContext() {}

func NewIdentifierContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *IdentifierContext {
	var p = new(IdentifierContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_identifier

	return p
}

func (s *IdentifierContext) GetParser() antlr.Parser { return s.parser }

func (s *IdentifierContext) UNQUOTED_IDENTIFIER() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserUNQUOTED_IDENTIFIER, 0)
}

func (s *IdentifierContext) QUOTED_IDENTIFIER() antlr.TerminalNode {
	return s.GetToken(SimplifiedCqlParserQUOTED_IDENTIFIER, 0)
}

func (s *IdentifierContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *IdentifierContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *IdentifierContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterIdentifier(s)
	}
}

func (s *IdentifierContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitIdentifier(s)
	}
}

func (p *SimplifiedCqlParser) Identifier() (localctx IIdentifierContext) {
	localctx = NewIdentifierContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 48, SimplifiedCqlParserRULE_identifier)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(268)
		_la = p.GetTokenStream().LA(1)

		if !(_la == SimplifiedCqlParserQUOTED_IDENTIFIER || _la == SimplifiedCqlParserUNQUOTED_IDENTIFIER) {
			p.GetErrorHandler().RecoverInline(p)
		} else {
			p.GetErrorHandler().ReportMatch(p)
			p.Consume()
		}
	}

	return localctx
}

// IDiscardedContentContext is an interface to support dynamic dispatch.
type IDiscardedContentContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsDiscardedContentContext differentiates from other interfaces.
	IsDiscardedContentContext()
}

type DiscardedContentContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyDiscardedContentContext() *DiscardedContentContext {
	var p = new(DiscardedContentContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_discardedContent
	return p
}

func (*DiscardedContentContext) IsDiscardedContentContext() {}

func NewDiscardedContentContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *DiscardedContentContext {
	var p = new(DiscardedContentContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_discardedContent

	return p
}

func (s *DiscardedContentContext) GetParser() antlr.Parser { return s.parser }

func (s *DiscardedContentContext) AllUnknown() []IUnknownContext {
	var ts = s.GetTypedRuleContexts(reflect.TypeOf((*IUnknownContext)(nil)).Elem())
	var tst = make([]IUnknownContext, len(ts))

	for i, t := range ts {
		if t != nil {
			tst[i] = t.(IUnknownContext)
		}
	}

	return tst
}

func (s *DiscardedContentContext) Unknown(i int) IUnknownContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IUnknownContext)(nil)).Elem(), i)

	if t == nil {
		return nil
	}

	return t.(IUnknownContext)
}

func (s *DiscardedContentContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *DiscardedContentContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *DiscardedContentContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterDiscardedContent(s)
	}
}

func (s *DiscardedContentContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitDiscardedContent(s)
	}
}

func (p *SimplifiedCqlParser) DiscardedContent() (localctx IDiscardedContentContext) {
	localctx = NewDiscardedContentContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 50, SimplifiedCqlParserRULE_discardedContent)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	var _alt int

	p.EnterOuterAlt(localctx, 1)
	p.SetState(273)
	p.GetErrorHandler().Sync(p)
	_alt = p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 27, p.GetParserRuleContext())

	for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
		if _alt == 1 {
			{
				p.SetState(270)
				p.Unknown()
			}

		}
		p.SetState(275)
		p.GetErrorHandler().Sync(p)
		_alt = p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 27, p.GetParserRuleContext())
	}

	return localctx
}

// IUnknownContext is an interface to support dynamic dispatch.
type IUnknownContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsUnknownContext differentiates from other interfaces.
	IsUnknownContext()
}

type UnknownContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyUnknownContext() *UnknownContext {
	var p = new(UnknownContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = SimplifiedCqlParserRULE_unknown
	return p
}

func (*UnknownContext) IsUnknownContext() {}

func NewUnknownContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *UnknownContext {
	var p = new(UnknownContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = SimplifiedCqlParserRULE_unknown

	return p
}

func (s *UnknownContext) GetParser() antlr.Parser { return s.parser }
func (s *UnknownContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *UnknownContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *UnknownContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.EnterUnknown(s)
	}
}

func (s *UnknownContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(SimplifiedCqlListener); ok {
		listenerT.ExitUnknown(s)
	}
}

func (p *SimplifiedCqlParser) Unknown() (localctx IUnknownContext) {
	localctx = NewUnknownContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 52, SimplifiedCqlParserRULE_unknown)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	p.SetState(276)
	p.MatchWildcard()

	return localctx
}
