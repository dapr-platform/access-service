package service

import (
	"access-service/entity"
	"access-service/model"
	"context"
	"encoding/json"
	"github.com/dapr-platform/common"
)

type DefaultMqttProvider struct {
}

func (y *DefaultMqttProvider) ProcessDataByGatewayPayload(gatewayName string, payload []byte) (datas []entity.DeviceDataInfo, err error) {
	rawData := make(map[string]any, 0)
	err = json.Unmarshal(payload, &rawData)
	if err != nil {
		common.Logger.Error("DefaultMqttProvider ProcessDataByGatewayPayload Unmarshal error ", err)
		return
	}
	gatewayId := common.GetMD5Hash(gatewayName)

	deviceDataInfoMap := make(map[string]entity.DeviceDataInfo, 0)
	for k, v := range rawData {
		_, _ = k, v
		//TODO
	}
	for _, v := range deviceDataInfoMap {
		datas = append(datas, v)
	}
	gateway, err := common.DbGetOne[model.Device](context.Background(), common.GetDaprClient(), model.DeviceTableInfo.Name, model.Device_FIELD_NAME_id+"="+gatewayId)
	if err != nil {
		common.Logger.Error(gatewayName + " can't find gateway by " + gatewayName + " err=" + err.Error())
	} else if gateway == nil {
		common.Logger.Error(gatewayName + " can't find gateway by " + gatewayName)
	} else {
		gatewayInfo := entity.DeviceDataInfo{
			DeviceIdentifier: gateway.Identifier,
			Points:           make([]entity.PointData, 0),
		}
		datas = append(datas, gatewayInfo)
	}

	//

	return
}

func (y *DefaultMqttProvider) ProcessDataByDevicePayload(gatewayId string, payload []byte) (datas []entity.DeviceDataInfo, err error) {
	//TODO implement me
	panic("implement me")
}

func (y *DefaultMqttProvider) GetName() string {
	return "default"
}
func (y *DefaultMqttProvider) getId(parentId, childId string) string {
	return common.GetMD5Hash(y.GetName() + "_" + parentId + "_" + childId)
}
