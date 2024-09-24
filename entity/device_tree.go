package entity

import "github.com/dapr-platform/common"

type Device_tree struct {
	ID               string           `json:"id"`                //id
	CreatedBy        string           `json:"created_by"`        //created_by
	CreatedTime      common.LocalTime `json:"created_time"`      //created_time
	UpdatedBy        string           `json:"updated_by"`        //updated_by
	UpdatedTime      common.LocalTime `json:"updated_time"`      //updated_time
	Name             string           `json:"name"`              //name
	Type             int32            `json:"type"`              //type
	ParentID         string           `json:"parent_id"`         //parent_id
	ProjectID        string           `json:"project_id"`        //project_id
	ManageAttributes string           `json:"manage_attributes"` //manage_attributes
	ProtocolID       string           `json:"protocol_id"`       //protocol_id
	ProtocolConfig   string           `json:"protocol_config"`   //protocol_config
	DeviceMetaID     string           `json:"device_meta_id"`    //device_meta_id
	DeviceID         string           `json:"device_id"`         //device_id
	Status           int32            `json:"status"`            //status
	Initialized      int32            `json:"initialized"`       //initialized
	Haschild         int32            `json:"haschild"`          //haschild
	Children         []Device_tree    `json:"children"`          //children

}
