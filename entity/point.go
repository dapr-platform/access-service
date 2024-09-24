package entity

import "github.com/dapr-platform/common"

type PointData struct {
	ID    string           `json:"id"`    //点位id
	Ts    common.LocalTime `json:"ts"`    //创建时间
	Key   string           `json:"key"`   //key
	Value any              `json:"value"` //值

}
