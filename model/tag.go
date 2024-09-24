package model

import (
	"database/sql"
	"github.com/dapr-platform/common"
	"time"
)

var (
	_ = time.Second
	_ = sql.LevelDefault
	_ = common.LocalTime{}
)

/*
DB Table Details
-------------------------------------


Table: o_tag
[ 0] id                                             VARCHAR(32)          null: false  primary: true   isArray: false  auto: false  col: VARCHAR         len: 32      default: []
[ 1] rel_id                                         VARCHAR(255)         null: false  primary: false  isArray: false  auto: false  col: VARCHAR         len: 255     default: []
[ 2] key                                            VARCHAR(255)         null: false  primary: false  isArray: false  auto: false  col: VARCHAR         len: 255     default: []
[ 3] value                                          VARCHAR(255)         null: false  primary: false  isArray: false  auto: false  col: VARCHAR         len: 255     default: []
[ 4] editable                                       INT4                 null: false  primary: false  isArray: false  auto: false  col: INT4            len: -1      default: []
[ 5] rel_type                                       INT4                 null: true   primary: false  isArray: false  auto: false  col: INT4            len: -1      default: []
[ 6] user_id                                        VARCHAR(32)          null: false  primary: false  isArray: false  auto: false  col: VARCHAR         len: 32      default: []


JSON Sample
-------------------------------------
{    "id": "CxiKAUdqsKGpdSqkvZZDFFkxw",    "rel_id": "PuSkBuQHbQwrJedOnYejfoTWg",    "key": "qZTlVsqgKiETVgAbCufFKxuZX",    "value": "ZSjkYdEwIlRpnRSFKMHGiBJIS",    "editable": 26,    "rel_type": 61,    "user_id": "ILOaTuinlbaprLVhUnXqEvFmh"}



*/

var (
	Tag_FIELD_NAME_id = "id"

	Tag_FIELD_NAME_rel_id = "rel_id"

	Tag_FIELD_NAME_key = "key"

	Tag_FIELD_NAME_value = "value"

	Tag_FIELD_NAME_editable = "editable"

	Tag_FIELD_NAME_rel_type = "rel_type"

	Tag_FIELD_NAME_user_id = "user_id"
)

// Tag struct is a row record of the o_tag table in the  database
type Tag struct {
	ID       string `json:"id"`       //唯一标识
	RelID    string `json:"rel_id"`   //关联id
	Key      string `json:"key"`      //名称
	Value    string `json:"value"`    //值
	Editable int32  `json:"editable"` //是否可编辑（导入的不可编辑）
	RelType  int32  `json:"rel_type"` //(1:网关、2:设备、3:点位
	UserID   string `json:"user_id"`  //用户id

}

var TagTableInfo = &TableInfo{
	Name: "o_tag",
	Columns: []*ColumnInfo{

		&ColumnInfo{
			Index:              0,
			Name:               "id",
			Comment:            `唯一标识`,
			Notes:              ``,
			Nullable:           false,
			DatabaseTypeName:   "VARCHAR",
			DatabaseTypePretty: "VARCHAR(32)",
			IsPrimaryKey:       true,
			IsAutoIncrement:    false,
			IsArray:            false,
			ColumnType:         "VARCHAR",
			ColumnLength:       32,
			GoFieldName:        "ID",
			GoFieldType:        "string",
			JSONFieldName:      "id",
			ProtobufFieldName:  "id",
			ProtobufType:       "string",
			ProtobufPos:        1,
		},

		&ColumnInfo{
			Index:              1,
			Name:               "rel_id",
			Comment:            `关联id`,
			Notes:              ``,
			Nullable:           false,
			DatabaseTypeName:   "VARCHAR",
			DatabaseTypePretty: "VARCHAR(255)",
			IsPrimaryKey:       false,
			IsAutoIncrement:    false,
			IsArray:            false,
			ColumnType:         "VARCHAR",
			ColumnLength:       255,
			GoFieldName:        "RelID",
			GoFieldType:        "string",
			JSONFieldName:      "rel_id",
			ProtobufFieldName:  "rel_id",
			ProtobufType:       "string",
			ProtobufPos:        2,
		},

		&ColumnInfo{
			Index:              2,
			Name:               "key",
			Comment:            `名称`,
			Notes:              ``,
			Nullable:           false,
			DatabaseTypeName:   "VARCHAR",
			DatabaseTypePretty: "VARCHAR(255)",
			IsPrimaryKey:       false,
			IsAutoIncrement:    false,
			IsArray:            false,
			ColumnType:         "VARCHAR",
			ColumnLength:       255,
			GoFieldName:        "Key",
			GoFieldType:        "string",
			JSONFieldName:      "key",
			ProtobufFieldName:  "key",
			ProtobufType:       "string",
			ProtobufPos:        3,
		},

		&ColumnInfo{
			Index:              3,
			Name:               "value",
			Comment:            `值`,
			Notes:              ``,
			Nullable:           false,
			DatabaseTypeName:   "VARCHAR",
			DatabaseTypePretty: "VARCHAR(255)",
			IsPrimaryKey:       false,
			IsAutoIncrement:    false,
			IsArray:            false,
			ColumnType:         "VARCHAR",
			ColumnLength:       255,
			GoFieldName:        "Value",
			GoFieldType:        "string",
			JSONFieldName:      "value",
			ProtobufFieldName:  "value",
			ProtobufType:       "string",
			ProtobufPos:        4,
		},

		&ColumnInfo{
			Index:              4,
			Name:               "editable",
			Comment:            `是否可编辑（导入的不可编辑）`,
			Notes:              ``,
			Nullable:           false,
			DatabaseTypeName:   "INT4",
			DatabaseTypePretty: "INT4",
			IsPrimaryKey:       false,
			IsAutoIncrement:    false,
			IsArray:            false,
			ColumnType:         "INT4",
			ColumnLength:       -1,
			GoFieldName:        "Editable",
			GoFieldType:        "int32",
			JSONFieldName:      "editable",
			ProtobufFieldName:  "editable",
			ProtobufType:       "int32",
			ProtobufPos:        5,
		},

		&ColumnInfo{
			Index:              5,
			Name:               "rel_type",
			Comment:            `(1:网关、2:设备、3:点位`,
			Notes:              ``,
			Nullable:           true,
			DatabaseTypeName:   "INT4",
			DatabaseTypePretty: "INT4",
			IsPrimaryKey:       false,
			IsAutoIncrement:    false,
			IsArray:            false,
			ColumnType:         "INT4",
			ColumnLength:       -1,
			GoFieldName:        "RelType",
			GoFieldType:        "int32",
			JSONFieldName:      "rel_type",
			ProtobufFieldName:  "rel_type",
			ProtobufType:       "int32",
			ProtobufPos:        6,
		},

		&ColumnInfo{
			Index:              6,
			Name:               "user_id",
			Comment:            `用户id`,
			Notes:              ``,
			Nullable:           false,
			DatabaseTypeName:   "VARCHAR",
			DatabaseTypePretty: "VARCHAR(32)",
			IsPrimaryKey:       false,
			IsAutoIncrement:    false,
			IsArray:            false,
			ColumnType:         "VARCHAR",
			ColumnLength:       32,
			GoFieldName:        "UserID",
			GoFieldType:        "string",
			JSONFieldName:      "user_id",
			ProtobufFieldName:  "user_id",
			ProtobufType:       "string",
			ProtobufPos:        7,
		},
	},
}

// TableName sets the insert table name for this struct type
func (t *Tag) TableName() string {
	return "o_tag"
}

// BeforeSave invoked before saving, return an error if field is not populated.
func (t *Tag) BeforeSave() error {
	return nil
}

// Prepare invoked before saving, can be used to populate fields etc.
func (t *Tag) Prepare() {
}

// Validate invoked before performing action, return an error if field is not populated.
func (t *Tag) Validate(action Action) error {
	return nil
}

// TableInfo return table meta data
func (t *Tag) TableInfo() *TableInfo {
	return TagTableInfo
}
