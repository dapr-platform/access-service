package service

import (
	"access-service/model"
	"context"
	"encoding/json"
	"github.com/dapr-platform/common"
	"github.com/pkg/errors"
	"time"
)

type YhGatewayMsg struct {
	Name    string               `json:"name"`
	Type    string               `json:"type"`
	Model   string               `json:"model"`
	Devices []YhGatewayDeviceMsg `json:"devices"`
}
type YhGatewayDeviceMsg struct {
	Name       string              `json:"name"`
	Type       string              `json:"type"`
	Model      string              `json:"model"`
	Attributes []YhDeviceAttribute `json:"attributes"`
}
type YhDeviceAttribute struct {
	Name  string  `json:"name"`
	Tag   string  `json:"tag"`
	Value float64 `json:"value"`
}

type YanhuaProvider struct {
}

func (y *YanhuaProvider) ProcessDataByGatewayPayload(gatewayId string, payload []byte) (datas []model.Point_data, err error) {
	yhgwMsg := YhGatewayMsg{}
	err = json.Unmarshal(payload, &yhgwMsg)
	if err != nil {
		common.Logger.Error("ProcessDataByGatewayPayload Unmarshal error ", err)
		return
	}
	ts := common.LocalTime(time.Now())
	for _, dev := range yhgwMsg.Devices {
		id := y.getId(gatewayId, dev.Name)
		for _, attr := range dev.Attributes {
			data := model.Point_data{
				ID:    id,
				Ts:    ts,
				Key:   attr.Name,
				Value: attr.Value,
			}
			datas = append(datas, data)
			/*
				attrData := model.Device_attributes{
					ID:            y.getId(id, attr.Identifier),
					UpdatedBy:     "system",
					UpdatedTime:   common.LocalTime(time.Now()),
					DeviceID:      id,
					AttributeName: attr.Identifier,
					Tag:           attr.Tag,
					Value:         float32(attr.Value),
				}
				attrDatas = append(attrDatas, attrData)

			*/
		}
	}
	return
}

func (y *YanhuaProvider) ProcessDataByDevicePayload(gatewayId string, payload []byte) (datas []model.Point_data, err error) {
	//TODO implement me
	panic("implement me")
}

func (y *YanhuaProvider) DiscoveryByDevicePayload(gatewayId string, payload []byte) (devices []model.Device, err error) {
	//TODO implement me
	panic("implement me")
}

func (y *YanhuaProvider) GetName() string {
	return "yh"
}
func (y *YanhuaProvider) getId(parentId, childId string) string {
	return common.GetMD5Hash(y.GetName() + "_" + parentId + "_" + childId)
}

func (y *YanhuaProvider) DiscoveryByGatewayPayload(gatewayId string, payload []byte) (devices []model.Device, err error) {

	var yhGwMsg YhGatewayMsg
	err = json.Unmarshal(payload, &yhGwMsg)
	if err != nil {
		err = errors.Wrap(err, "yh gateway json unmarshal error ")
		return
	}
	mGatewayId := y.getId(gatewayId, "")
	//TODO 使用redis，减少判断的频次。
	existGateway, err := common.DbGetOne[model.Device](context.Background(), common.GetDaprClient(), model.DeviceTableInfo.Name, model.Device_FIELD_NAME_id+"="+mGatewayId)
	if err != nil {
		err = errors.Wrap(err, "DbGetOne error")
		return
	}
	if existGateway != nil {
		existGateway.UpdatedTime = common.LocalTime(time.Now())
		err = common.DbUpsert[model.Device](context.Background(), common.GetDaprClient(), *existGateway, model.DeviceTableInfo.Name, model.Device_FIELD_NAME_id)
		if err != nil {
			err = errors.Wrap(err, "DbUpsert error")
			return
		}
	} else {
		attrs := make(map[string]string, 0)
		attrs["model"] = yhGwMsg.Model
		attrs["type"] = yhGwMsg.Type
		existGateway = &model.Device{
			ID:          mGatewayId,
			Name:        yhGwMsg.Name,
			Type:        2, //1:device,2:gateway
			CreatedTime: common.LocalTime(time.Now()),
			ParentID:    "0",
			Status:      1,
		}
		_, err = common.DbInsert[model.Device](context.Background(), common.GetDaprClient(), *existGateway, model.DeviceTableInfo.Name)
		if err != nil {
			err = errors.Wrap(err, "DbInsert yhgateway error")
			return
		}

	}
	for _, dev := range yhGwMsg.Devices {

		id := y.getId(gatewayId, dev.Name)
		existDevice, err1 := common.DbGetOne[model.Device](context.Background(), common.GetDaprClient(), model.DeviceTableInfo.Name, model.Device_FIELD_NAME_id+"="+id)
		if err1 != nil {
			err = errors.Wrap(err1, "DbGetOne device error")
			return
		}
		if existDevice != nil {
			existDevice.UpdatedTime = common.LocalTime(time.Now())
			err = common.DbUpsert[model.Device](context.Background(), common.GetDaprClient(), *existDevice, model.DeviceTableInfo.Name, model.Device_FIELD_NAME_id)
			if err != nil {
				err = errors.Wrap(err, "DbUpsert error")
				return
			}
		} else {
			devAttrs := make(map[string]string, 0)
			devAttrs["model"] = dev.Model
			devAttrs["type"] = dev.Type

			device := &model.Device{
				ID:          id,
				Name:        dev.Name,
				Type:        1, //1:device,2:gateway
				CreatedTime: common.LocalTime(time.Now()),
				ParentID:    mGatewayId,
				Status:      1,
			}
			_, err = common.DbInsert[model.Device](context.Background(), common.GetDaprClient(), *device, model.DeviceTableInfo.Name)
			if err != nil {
				err = errors.Wrap(err, "DbInsert yhgateway error")
				return
			}
		}

	}

	return
}
