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


Table: v_device_with_points
[ 0] id                                             VARCHAR(32)          null: true   primary: false  isArray: false  auto: false  col: VARCHAR         len: 32      default: []
[ 1] identifier                                     VARCHAR(255)         null: true   primary: false  isArray: false  auto: false  col: VARCHAR         len: 255     default: []
[ 2] product_id                                     VARCHAR(32)          null: true   primary: false  isArray: false  auto: false  col: VARCHAR         len: 32      default: []
[ 3] enabled                                        INT4                 null: true   primary: false  isArray: false  auto: false  col: INT4            len: -1      default: []
[ 4] type                                           INT4                 null: true   primary: false  isArray: false  auto: false  col: INT4            len: -1      default: []
[ 5] tags                                           _TEXT                null: true   primary: false  isArray: false  auto: false  col: _TEXT           len: -1      default: []
[ 6] points                                         JSON                 null: true   primary: false  isArray: false  auto: false  col: JSON            len: -1      default: []


JSON Sample
-------------------------------------
{    "id": "XyFmpguETHBFXlJuPXewowjdS",    "identifier": "CBAJAjmVxkVOxrvyJRkMBwfHa",    "product_id": "qkiCFmQMbJpYCWWepGNBDDTOB",    "enabled": 48,    "type": 5,    "tags": "uJEBjUyFRLwGGvouHSJmqxYiR",    "points": 81}


Comments
-------------------------------------
[ 0] Warning table: v_device_with_points does not have a primary key defined, setting col position 1 id as primary key
Warning table: v_device_with_points primary key column id is nullable column, setting it as NOT NULL




*/

var (
	Device_with_points_FIELD_NAME_id = "id"

	Device_with_points_FIELD_NAME_identifier = "identifier"

	Device_with_points_FIELD_NAME_product_id = "product_id"

	Device_with_points_FIELD_NAME_enabled = "enabled"

	Device_with_points_FIELD_NAME_type = "type"

	Device_with_points_FIELD_NAME_tags = "tags"

	Device_with_points_FIELD_NAME_points = "points"
)

// Device_with_points struct is a row record of the v_device_with_points table in the  database
type Device_with_points struct {
	ID         string `json:"id"`         //id
	Identifier string `json:"identifier"` //identifier
	ProductID  string `json:"product_id"` //product_id
	Enabled    int32  `json:"enabled"`    //enabled
	Type       int32  `json:"type"`       //type
	Tags       string `json:"tags"`       //tags
	Points     any    `json:"points"`     //points

}

var Device_with_pointsTableInfo = &TableInfo{
	Name: "v_device_with_points",
	Columns: []*ColumnInfo{

		&ColumnInfo{
			Index:   0,
			Name:    "id",
			Comment: `id`,
			Notes: `Warning table: v_device_with_points does not have a primary key defined, setting col position 1 id as primary key
Warning table: v_device_with_points primary key column id is nullable column, setting it as NOT NULL
`,
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
			Name:               "identifier",
			Comment:            `identifier`,
			Notes:              ``,
			Nullable:           true,
			DatabaseTypeName:   "VARCHAR",
			DatabaseTypePretty: "VARCHAR(255)",
			IsPrimaryKey:       false,
			IsAutoIncrement:    false,
			IsArray:            false,
			ColumnType:         "VARCHAR",
			ColumnLength:       255,
			GoFieldName:        "Identifier",
			GoFieldType:        "string",
			JSONFieldName:      "identifier",
			ProtobufFieldName:  "identifier",
			ProtobufType:       "string",
			ProtobufPos:        2,
		},

		&ColumnInfo{
			Index:              2,
			Name:               "product_id",
			Comment:            `product_id`,
			Notes:              ``,
			Nullable:           true,
			DatabaseTypeName:   "VARCHAR",
			DatabaseTypePretty: "VARCHAR(32)",
			IsPrimaryKey:       false,
			IsAutoIncrement:    false,
			IsArray:            false,
			ColumnType:         "VARCHAR",
			ColumnLength:       32,
			GoFieldName:        "ProductID",
			GoFieldType:        "string",
			JSONFieldName:      "product_id",
			ProtobufFieldName:  "product_id",
			ProtobufType:       "string",
			ProtobufPos:        3,
		},

		&ColumnInfo{
			Index:              3,
			Name:               "enabled",
			Comment:            `enabled`,
			Notes:              ``,
			Nullable:           true,
			DatabaseTypeName:   "INT4",
			DatabaseTypePretty: "INT4",
			IsPrimaryKey:       false,
			IsAutoIncrement:    false,
			IsArray:            false,
			ColumnType:         "INT4",
			ColumnLength:       -1,
			GoFieldName:        "Enabled",
			GoFieldType:        "int32",
			JSONFieldName:      "enabled",
			ProtobufFieldName:  "enabled",
			ProtobufType:       "int32",
			ProtobufPos:        4,
		},

		&ColumnInfo{
			Index:              4,
			Name:               "type",
			Comment:            `type`,
			Notes:              ``,
			Nullable:           true,
			DatabaseTypeName:   "INT4",
			DatabaseTypePretty: "INT4",
			IsPrimaryKey:       false,
			IsAutoIncrement:    false,
			IsArray:            false,
			ColumnType:         "INT4",
			ColumnLength:       -1,
			GoFieldName:        "Type",
			GoFieldType:        "int32",
			JSONFieldName:      "type",
			ProtobufFieldName:  "type",
			ProtobufType:       "int32",
			ProtobufPos:        5,
		},

		&ColumnInfo{
			Index:              5,
			Name:               "tags",
			Comment:            `tags`,
			Notes:              ``,
			Nullable:           true,
			DatabaseTypeName:   "_TEXT",
			DatabaseTypePretty: "_TEXT",
			IsPrimaryKey:       false,
			IsAutoIncrement:    false,
			IsArray:            true,
			ColumnType:         "_TEXT",
			ColumnLength:       -1,
			GoFieldName:        "Tags",
			GoFieldType:        "string",
			JSONFieldName:      "tags",
			ProtobufFieldName:  "tags",
			ProtobufType:       "string",
			ProtobufPos:        6,
		},

		&ColumnInfo{
			Index:              6,
			Name:               "points",
			Comment:            `points`,
			Notes:              ``,
			Nullable:           true,
			DatabaseTypeName:   "JSON",
			DatabaseTypePretty: "JSON",
			IsPrimaryKey:       false,
			IsAutoIncrement:    false,
			IsArray:            false,
			ColumnType:         "JSON",
			ColumnLength:       -1,
			GoFieldName:        "Points",
			GoFieldType:        "any",
			JSONFieldName:      "points",
			ProtobufFieldName:  "points",
			ProtobufType:       "string",
			ProtobufPos:        7,
		},
	},
}

// TableName sets the insert table name for this struct type
func (d *Device_with_points) TableName() string {
	return "v_device_with_points"
}

// BeforeSave invoked before saving, return an error if field is not populated.
func (d *Device_with_points) BeforeSave() error {
	return nil
}

// Prepare invoked before saving, can be used to populate fields etc.
func (d *Device_with_points) Prepare() {
}

// Validate invoked before performing action, return an error if field is not populated.
func (d *Device_with_points) Validate(action Action) error {
	return nil
}

// TableInfo return table meta data
func (d *Device_with_points) TableInfo() *TableInfo {
	return Device_with_pointsTableInfo
}
