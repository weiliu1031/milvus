// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

// TODO: fill it
// info for each blob
type BlobInfo struct {
	Length int
}

// InsertData example row_schema: {float_field, int_field, float_vector_field, string_field}
// Data {<0, row_id>, <1, timestamp>, <100, float_field>, <101, int_field>, <102, float_vector_field>, <103, string_field>}
//
// system filed id:
// 0: unique row id
// 1: timestamp
// 100: first user field id
// 101: second user field id
// 102: ...
type InsertData struct {
	// TODO, data should be zero copy by passing data directly to event reader or change Data to map[FieldID]FieldDataArray
	Data  map[FieldID]FieldData // field id to field data
	Infos []BlobInfo
}

func NewInsertData(schema *schemapb.CollectionSchema) (*InsertData, error) {
	if schema == nil {
		return nil, fmt.Errorf("Nil input schema")
	}

	idata := &InsertData{
		Data: make(map[FieldID]FieldData),
	}

	for _, fSchema := range schema.Fields {
		fieldData, err := NewFieldData(fSchema.DataType, fSchema)
		if err != nil {
			return nil, err
		}
		idata.Data[fSchema.FieldID] = fieldData
	}
	return idata, nil
}

func (iData *InsertData) IsEmpty() bool {
	if iData == nil {
		return true
	}

	timeFieldData, ok := iData.Data[common.TimeStampField]
	return (!ok) || (timeFieldData.RowNum() <= 0)
}

func (i *InsertData) GetRowNum() int {
	if i.Data == nil || len(i.Data) == 0 {
		return 0
	}

	data, ok := i.Data[common.RowIDField]
	if !ok {
		return 0
	}

	return data.RowNum()
}

func (i *InsertData) GetMemorySize() int {
	var size int
	if i.Data == nil || len(i.Data) == 0 {
		return size
	}

	for _, data := range i.Data {
		size += data.GetMemorySize()
	}

	return size
}

func (i *InsertData) Append(row map[FieldID]interface{}) error {
	for fID, v := range row {
		field, ok := i.Data[fID]
		if !ok {
			return fmt.Errorf("Missing field when appending row, got %d", fID)
		}

		if err := field.AppendRow(v); err != nil {
			return err
		}
	}

	return nil
}

// FieldData defines field data interface
type FieldData interface {
	GetMemorySize() int
	RowNum() int
	GetRow(i int) any
	AppendRow(row interface{}) error
	GetNullable() bool
}

func NewFieldData(dataType schemapb.DataType, fieldSchema *schemapb.FieldSchema) (FieldData, error) {
	typeParams := fieldSchema.GetTypeParams()
	switch dataType {
	case schemapb.DataType_Float16Vector:
		dim, err := GetDimFromParams(typeParams)
		if err != nil {
			return nil, err
		}
		return &Float16VectorFieldData{
			Data: make([]byte, 0),
			Dim:  dim,
		}, nil
	case schemapb.DataType_FloatVector:
		dim, err := GetDimFromParams(typeParams)
		if err != nil {
			return nil, err
		}
		return &FloatVectorFieldData{
			Data: make([]float32, 0),
			Dim:  dim,
		}, nil
	case schemapb.DataType_BinaryVector:
		dim, err := GetDimFromParams(typeParams)
		if err != nil {
			return nil, err
		}
		return &BinaryVectorFieldData{
			Data: make([]byte, 0),
			Dim:  dim,
		}, nil

	case schemapb.DataType_Bool:
		return &BoolFieldData{
			Data:      make([]bool, 0),
			ValidData: make([]bool, 0),
		}, nil

	case schemapb.DataType_Int8:
		return &Int8FieldData{
			Data:      make([]int8, 0),
			ValidData: make([]bool, 0),
		}, nil

	case schemapb.DataType_Int16:
		return &Int16FieldData{
			Data:      make([]int16, 0),
			ValidData: make([]bool, 0),
		}, nil

	case schemapb.DataType_Int32:
		return &Int32FieldData{
			Data:      make([]int32, 0),
			ValidData: make([]bool, 0),
		}, nil

	case schemapb.DataType_Int64:
		return &Int64FieldData{
			Data:      make([]int64, 0),
			ValidData: make([]bool, 0),
		}, nil
	case schemapb.DataType_Float:
		return &FloatFieldData{
			Data:      make([]float32, 0),
			ValidData: make([]bool, 0),
		}, nil

	case schemapb.DataType_Double:
		return &DoubleFieldData{
			Data:      make([]float64, 0),
			ValidData: make([]bool, 0),
		}, nil
	case schemapb.DataType_JSON:
		return &JSONFieldData{
			Data:      make([][]byte, 0),
			ValidData: make([]bool, 0),
		}, nil
	case schemapb.DataType_Array:
		return &ArrayFieldData{
			Data:        make([]*schemapb.ScalarField, 0),
			ElementType: fieldSchema.GetElementType(),
			ValidData:   make([]bool, 0),
		}, nil
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		return &StringFieldData{
			Data:      make([]string, 0),
			ValidData: make([]bool, 0),
		}, nil
	default:
		return nil, fmt.Errorf("Unexpected schema data type: %d", dataType)
	}
}

type BoolFieldData struct {
	Data      []bool
	ValidData []bool
}
type Int8FieldData struct {
	Data      []int8
	ValidData []bool
}
type Int16FieldData struct {
	Data      []int16
	ValidData []bool
}
type Int32FieldData struct {
	Data      []int32
	ValidData []bool
}
type Int64FieldData struct {
	Data      []int64
	ValidData []bool
}
type FloatFieldData struct {
	Data      []float32
	ValidData []bool
}
type DoubleFieldData struct {
	Data      []float64
	ValidData []bool
}
type StringFieldData struct {
	Data      []string
	ValidData []bool
}
type ArrayFieldData struct {
	ElementType schemapb.DataType
	Data        []*schemapb.ScalarField
	ValidData   []bool
}
type JSONFieldData struct {
	Data      [][]byte
	ValidData []bool
}
type BinaryVectorFieldData struct {
	Data []byte
	Dim  int
}
type FloatVectorFieldData struct {
	Data []float32
	Dim  int
}
type Float16VectorFieldData struct {
	Data []byte
	Dim  int
}

// RowNum implements FieldData.RowNum
func (data *BoolFieldData) RowNum() int          { return len(data.Data) }
func (data *Int8FieldData) RowNum() int          { return len(data.Data) }
func (data *Int16FieldData) RowNum() int         { return len(data.Data) }
func (data *Int32FieldData) RowNum() int         { return len(data.Data) }
func (data *Int64FieldData) RowNum() int         { return len(data.Data) }
func (data *FloatFieldData) RowNum() int         { return len(data.Data) }
func (data *DoubleFieldData) RowNum() int        { return len(data.Data) }
func (data *StringFieldData) RowNum() int        { return len(data.Data) }
func (data *ArrayFieldData) RowNum() int         { return len(data.Data) }
func (data *JSONFieldData) RowNum() int          { return len(data.Data) }
func (data *BinaryVectorFieldData) RowNum() int  { return len(data.Data) * 8 / data.Dim }
func (data *FloatVectorFieldData) RowNum() int   { return len(data.Data) / data.Dim }
func (data *Float16VectorFieldData) RowNum() int { return len(data.Data) / 2 / data.Dim }

// GetRow implements FieldData.GetRow
func (data *BoolFieldData) GetRow(i int) any   { return data.Data[i] }
func (data *Int8FieldData) GetRow(i int) any   { return data.Data[i] }
func (data *Int16FieldData) GetRow(i int) any  { return data.Data[i] }
func (data *Int32FieldData) GetRow(i int) any  { return data.Data[i] }
func (data *Int64FieldData) GetRow(i int) any  { return data.Data[i] }
func (data *FloatFieldData) GetRow(i int) any  { return data.Data[i] }
func (data *DoubleFieldData) GetRow(i int) any { return data.Data[i] }
func (data *StringFieldData) GetRow(i int) any { return data.Data[i] }
func (data *ArrayFieldData) GetRow(i int) any  { return data.Data[i] }
func (data *JSONFieldData) GetRow(i int) any   { return data.Data[i] }
func (data *BinaryVectorFieldData) GetRow(i int) interface{} {
	return data.Data[i*data.Dim/8 : (i+1)*data.Dim/8]
}

func (data *FloatVectorFieldData) GetRow(i int) interface{} {
	return data.Data[i*data.Dim : (i+1)*data.Dim]
}

func (data *Float16VectorFieldData) GetRow(i int) interface{} {
	return data.Data[i*data.Dim*2 : (i+1)*data.Dim*2]
}

// AppendRow implements FieldData.AppendRow
func (data *BoolFieldData) AppendRow(row interface{}) error {
	v, ok := row.(bool)
	if !ok {
		return merr.WrapErrParameterInvalid("bool", row, "Wrong row type")
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *Int8FieldData) AppendRow(row interface{}) error {
	v, ok := row.(int8)
	if !ok {
		return merr.WrapErrParameterInvalid("int8", row, "Wrong row type")
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *Int16FieldData) AppendRow(row interface{}) error {
	v, ok := row.(int16)
	if !ok {
		return merr.WrapErrParameterInvalid("int16", row, "Wrong row type")
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *Int32FieldData) AppendRow(row interface{}) error {
	v, ok := row.(int32)
	if !ok {
		return merr.WrapErrParameterInvalid("int32", row, "Wrong row type")
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *Int64FieldData) AppendRow(row interface{}) error {
	v, ok := row.(int64)
	if !ok {
		return merr.WrapErrParameterInvalid("int64", row, "Wrong row type")
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *FloatFieldData) AppendRow(row interface{}) error {
	v, ok := row.(float32)
	if !ok {
		return merr.WrapErrParameterInvalid("float32", row, "Wrong row type")
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *DoubleFieldData) AppendRow(row interface{}) error {
	v, ok := row.(float64)
	if !ok {
		return merr.WrapErrParameterInvalid("float64", row, "Wrong row type")
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *StringFieldData) AppendRow(row interface{}) error {
	v, ok := row.(string)
	if !ok {
		return merr.WrapErrParameterInvalid("string", row, "Wrong row type")
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *ArrayFieldData) AppendRow(row interface{}) error {
	v, ok := row.(*schemapb.ScalarField)
	if !ok {
		return merr.WrapErrParameterInvalid("*schemapb.ScalarField", row, "Wrong row type")
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *JSONFieldData) AppendRow(row interface{}) error {
	v, ok := row.([]byte)
	if !ok {
		return merr.WrapErrParameterInvalid("[]byte", row, "Wrong row type")
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *BinaryVectorFieldData) AppendRow(row interface{}) error {
	v, ok := row.([]byte)
	if !ok || len(v) != data.Dim/8 {
		return merr.WrapErrParameterInvalid("[]byte", row, "Wrong row type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *FloatVectorFieldData) AppendRow(row interface{}) error {
	v, ok := row.([]float32)
	if !ok || len(v) != data.Dim {
		return merr.WrapErrParameterInvalid("[]float32", row, "Wrong row type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *Float16VectorFieldData) AppendRow(row interface{}) error {
	v, ok := row.([]byte)
	if !ok || len(v) != data.Dim*2 {
		return merr.WrapErrParameterInvalid("[]byte", row, "Wrong row type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

// GetMemorySize implements FieldData.GetMemorySize
func (data *BoolFieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData)
}
func (data *Int8FieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData)
}
func (data *Int16FieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData)
}
func (data *Int32FieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData)
}
func (data *Int64FieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData)
}
func (data *FloatFieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData)
}
func (data *DoubleFieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData)
}
func (data *BinaryVectorFieldData) GetMemorySize() int  { return binary.Size(data.Data) + 4 }
func (data *FloatVectorFieldData) GetMemorySize() int   { return binary.Size(data.Data) + 4 }
func (data *Float16VectorFieldData) GetMemorySize() int { return binary.Size(data.Data) + 4 }

// why not binary.Size(data) directly? binary.Size(data) return -1
// binary.Size returns how many bytes Write would generate to encode the value v, which
// must be a fixed-size value or a slice of fixed-size values, or a pointer to such data.
// If v is neither of these, binary.Size returns -1.
func (data *StringFieldData) GetMemorySize() int {
	var size int
	for _, val := range data.Data {
		size += len(val) + 16
	}
	return size
}

func (data *ArrayFieldData) GetMemorySize() int {
	var size int
	for _, val := range data.Data {
		switch data.ElementType {
		case schemapb.DataType_Bool:
			size += binary.Size(val.GetBoolData().GetData())
		case schemapb.DataType_Int8:
			size += binary.Size(val.GetIntData().GetData()) / 4
		case schemapb.DataType_Int16:
			size += binary.Size(val.GetIntData().GetData()) / 2
		case schemapb.DataType_Int32:
			size += binary.Size(val.GetIntData().GetData())
		case schemapb.DataType_Int64:
			size += binary.Size(val.GetLongData().GetData())
		case schemapb.DataType_Float:
			size += binary.Size(val.GetFloatData().GetData())
		case schemapb.DataType_Double:
			size += binary.Size(val.GetDoubleData().GetData())
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			size += (&StringFieldData{Data: val.GetStringData().GetData()}).GetMemorySize()
		}
	}
	return size
}

func (data *JSONFieldData) GetMemorySize() int {
	var size int
	for _, val := range data.Data {
		size += len(val) + 16
	}
	return size
}

// GetMemorySize implements FieldData.GetMemorySize
func (data *BoolFieldData) GetNullable() bool {
	return len(data.ValidData) != 0
}
func (data *Int8FieldData) GetNullable() bool {
	return len(data.ValidData) != 0
}
func (data *Int16FieldData) GetNullable() bool {
	return len(data.ValidData) != 0
}
func (data *Int32FieldData) GetNullable() bool {
	return len(data.ValidData) != 0
}
func (data *Int64FieldData) GetNullable() bool {
	return len(data.ValidData) != 0
}
func (data *FloatFieldData) GetNullable() bool {
	return len(data.ValidData) != 0
}
func (data *DoubleFieldData) GetNullable() bool {
	return len(data.ValidData) != 0
}
func (data *BinaryVectorFieldData) GetNullable() bool {
	return false
}
func (data *FloatVectorFieldData) GetNullable() bool {
	return false
}
func (data *Float16VectorFieldData) GetNullable() bool {
	return false
}
func (data *StringFieldData) GetNullable() bool {
	return len(data.ValidData) != 0
}

func (data *ArrayFieldData) GetNullable() bool {
	return len(data.ValidData) != 0
}

func (data *JSONFieldData) GetNullable() bool {
	return len(data.ValidData) != 0
}
