package main

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/tool/parquet-tools/schematool"
)

func ShowSchema(fn string, w io.Writer) error {
	fr, err := local.NewLocalFileReader(fn)
	if err != nil {
		return err
	}
	defer fr.Close()
	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		return err
	}
	defer pr.ReadStop()
	tree := schematool.CreateSchemaTree(pr.SchemaHandler.SchemaElements)
	fmt.Fprintln(w, "----- Json schema -----")
	fmt.Fprintf(w, "%s\n", tree.OutputJsonSchema())
	return nil
}

func ParquetTypeToParquetTypeStr(pT *parquet.Type, cT *parquet.ConvertedType) (string, string) {
	var pTStr, cTStr string
	if pT != nil {
		switch *pT {
		case parquet.Type_BOOLEAN:
			pTStr = "BOOLEAN"
		case parquet.Type_INT32:
			pTStr = "INT32"
		case parquet.Type_INT64:
			pTStr = "INT64"
		case parquet.Type_INT96:
			pTStr = "INT96"
		case parquet.Type_FLOAT:
			pTStr = "FLOAT"
		case parquet.Type_DOUBLE:
			pTStr = "DOUBLE"
		case parquet.Type_BYTE_ARRAY:
			pTStr = "BYTE_ARRAY"
		case parquet.Type_FIXED_LEN_BYTE_ARRAY:
			pTStr = "FIXED_LEN_BYTE_ARRAY"
		}
	}
	if cT != nil {
		switch *cT {
		case parquet.ConvertedType_UTF8:
			cTStr = "UTF8"
		case parquet.ConvertedType_INT_8:
			cTStr = "INT_8"
		case parquet.ConvertedType_INT_16:
			cTStr = "INT_16"
		case parquet.ConvertedType_INT_32:
			cTStr = "INT_32"
		case parquet.ConvertedType_INT_64:
			cTStr = "INT_64"
		case parquet.ConvertedType_UINT_8:
			cTStr = "UINT_8"
		case parquet.ConvertedType_UINT_16:
			cTStr = "UINT_16"
		case parquet.ConvertedType_UINT_32:
			cTStr = "UINT_32"
		case parquet.ConvertedType_UINT_64:
			cTStr = "UINT_64"
		case parquet.ConvertedType_DATE:
			cTStr = "DATE"
		case parquet.ConvertedType_TIME_MILLIS:
			cTStr = "TIME_MILLIS"
		case parquet.ConvertedType_TIME_MICROS:
			cTStr = "TIME_MICROS"
		case parquet.ConvertedType_TIMESTAMP_MILLIS:
			cTStr = "TIMESTAMP_MILLIS"
		case parquet.ConvertedType_TIMESTAMP_MICROS:
			cTStr = "TIMESTAMP_MICROS"
		case parquet.ConvertedType_INTERVAL:
			cTStr = "INTERVAL"
		case parquet.ConvertedType_DECIMAL:
			cTStr = "DECIMAL"
		case parquet.ConvertedType_MAP:
			cTStr = "MAP"
		case parquet.ConvertedType_LIST:
			cTStr = "LIST"
		}
	}
	return pTStr, cTStr
}

type Node struct {
	Indent   string
	SE       *parquet.SchemaElement
	Children []*Node
}

func NewNode(schema *parquet.SchemaElement) *Node {
	node := &(Node{
		Indent:   "",
		SE:       schema,
		Children: []*Node{},
	})
	return node
}

func (self *Node) OutputJsonSchema() string {
	res := "{\"Tag\":"
	pT, cT := self.SE.Type, self.SE.ConvertedType
	rTStr := "REQUIRED"
	if self.SE.GetRepetitionType() == parquet.FieldRepetitionType_OPTIONAL {
		rTStr = "OPTIONAL"
	} else if self.SE.GetRepetitionType() == parquet.FieldRepetitionType_REPEATED {
		rTStr = "REPEATED"
	}

	pTStr, cTStr := ParquetTypeToParquetTypeStr(pT, cT)
	tagStr := "\"name=%s, type=%s, repetitiontype=%s\""
	name := self.SE.GetName()

	if len(self.Children) == 0 {
		if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY && cT == nil {
			length := self.SE.GetTypeLength()
			tagStr = "\"name=%s, type=%s, length=%d, repetitiontype=%s\""
			res += fmt.Sprintf(tagStr, name, pTStr, length, rTStr) + "}"

		} else if cT != nil && *cT == parquet.ConvertedType_DECIMAL {
			scale, precision := self.SE.GetScale(), self.SE.GetPrecision()
			if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
				length := self.SE.GetTypeLength()
				tagStr = "\"name=%s, type=%s, basetype=%s, scale=%d, precision=%d, length=%d, repetitiontype=%s\""
				res += fmt.Sprintf(tagStr, name, cTStr, pTStr, scale, precision, length, rTStr) + "}"
			} else {
				tagStr = "\"name=%s, type=%s, basetype=%s, scale=%d, precision=%d, repetitiontype\""
				res += fmt.Sprintf(tagStr, name, cTStr, pTStr, scale, precision, rTStr) + "}"
			}

		} else {
			typeStr := pTStr
			if cT != nil {
				typeStr = cTStr
			}
			res += fmt.Sprintf(tagStr, name, typeStr, rTStr) + "}"

		}
	} else {
		if cT != nil {
			tagStr = "\"name=%s, type=%s, repetitiontype=%s\""
			res += fmt.Sprintf(tagStr, name, cTStr, rTStr)
		} else {
			tagStr = "\"name=%s, repetitiontype=%s\""
			res += fmt.Sprintf(tagStr, name, rTStr)
		}
		res += ",\n"
		res += "\"Fields\":[\n"

		nodes := self.Children
		if cT != nil {
			nodes = self.Children[0].Children
		}

		for i := 0; i < len(nodes); i++ {
			cNode := nodes[i]
			if i == len(nodes)-1 {
				res += cNode.OutputJsonSchema() + "\n"
			} else {
				res += cNode.OutputJsonSchema() + ",\n"
			}
		}

		res += "]\n"
		res += "}"

	}
	return res
}

type SchemaTree struct {
	Root *Node
}

func CreateSchemaTree(schemas []*parquet.SchemaElement) *SchemaTree {
	pos := 0
	stack := make([]*Node, 0)
	root := NewNode(schemas[0])
	stack = append(stack, root)
	pos++

	for len(stack) > 0 {
		node := stack[len(stack)-1]
		numChildren := int(node.SE.GetNumChildren())
		lc := len(node.Children)
		if lc < numChildren {
			newNode := NewNode(schemas[pos])
			newNode.Indent += "  "
			node.Children = append(node.Children, newNode)
			stack = append(stack, newNode)
			pos++
		} else {
			stack = stack[:len(stack)-1]
		}
	}

	st := new(SchemaTree)
	st.Root = root
	return st
}

func (self *SchemaTree) OutputJsonSchema() string {
	jsonStr := self.Root.OutputJsonSchema()
	var obj schema.JSONSchemaItemType
	json.Unmarshal([]byte(jsonStr), &obj)
	res, _ := json.MarshalIndent(&obj, "", "  ")
	return string(res)
}
