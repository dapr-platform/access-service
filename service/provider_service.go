package service

import (
	"access-service/entity"
)

type Provider interface {
	GetName() string
	ProcessDataByGatewayPayload(gatewayId string, payload []byte) (datas []entity.DeviceDataInfo, err error)
	ProcessDataByDevicePayload(gatewayId string, payload []byte) (datas []entity.DeviceDataInfo, err error)
}

// 厂商处理逻辑
var providers map[string]Provider

func init() {
	providers = make(map[string]Provider, 0)
	//yhProvider := &YanhuaProvider{}
	//providers[yhProvider.GetName()] = yhProvider
	defaultMqttProvider := &DefaultMqttProvider{}
	providers[defaultMqttProvider.GetName()] = defaultMqttProvider
}

func GetProvider(name string) (Provider, bool) {
	p, exists := providers[name]
	return p, exists
}
