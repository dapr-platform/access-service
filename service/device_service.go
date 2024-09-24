package service

import (
	"access-service/entity"
	"access-service/eventpub"
	"access-service/model"
	"context"
	"github.com/dapr-platform/common"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"strings"
)

func ProcessPropertySetMessage(ctx context.Context, event map[string]interface{}) (err error) {
	identifier, exist := event["device_identifier"].(string)
	if !exist {
		err = errors.New("device identifier not exist")
		return
	}
	qstr := "identifier=" + identifier
	device, err := common.DbGetOne[entity.DeviceWithPoints](ctx, common.GetDaprClient(), model.Device_with_pointsTableInfo.Name, qstr)
	if err != nil {
		err = errors.Wrap(err, "DbGetOne device error")
		return
	}
	if device == nil {
		err = errors.New("device not exist")
		return
	}
	if device.Enabled == 0 {
		err = errors.New("device disabled")
		//eventpub.ConstructAndSendEvent(context.Background(), common.EventTypeDevice, device.Identifier+" 禁用状态", device.Identifier+" 禁用状态, 不能设置属性", common.EventStatusActive, common.EventLevelWarning, time.Now(), device.Identifier, device.Identifier, "")
		return
	}
	propertyName, exist := event["property_name"].(string)
	if !exist {
		err = errors.New("property name not exist")
		return
	}
	value, exist := event["property_value"]
	if !exist {
		err = errors.New("property value not exist")
		return
	}
	success := false
	for _, point := range device.Points {
		if point.Identifier == propertyName {

			tags := point.Tags
			properties := make(map[string]any, 0)
			for _, tag := range tags {
				arr := strings.Split(tag, ":")
				if len(arr) == 2 {
					properties[arr[0]] = arr[1]
				}
			}
			protocolId, ok := properties["接入协议"]
			if !ok {
				common.Logger.Error(point.Identifier + " has no protocol")
				continue
			}
			accessType, ok := properties["接入类型"]
			if !ok {
				common.Logger.Error(point.Identifier + " has no access type")
				continue
			}
			deviceProcessor, err := GetDeviceProviderProcessor(cast.ToString(accessType))
			if err != nil {
				common.Logger.Error(point.Identifier + " GetDeviceProviderProcessor " + err.Error())
				continue
			}
			protocolProperties, err := deviceProcessor.GetCustomPropertiesForProtocol(properties)
			if err != nil {
				common.Logger.Error(point.Identifier + " getReadProperties " + err.Error())
				continue
			}
			err = ProtocolWriteValue(cast.ToString(protocolId), protocolProperties, value)
			if err != nil {
				common.Logger.Error(point.Identifier + " ProtocolReadValue " + err.Error())
				continue
			}
			success = true

			//发送写数据的信息。供监控使用
			pointWriteDataEvent := entity.PointRWDataMetaInfo{
				Type:             "w",
				DeviceIdentifier: device.Identifier,
				PointIdentifier:  propertyName,
				Value:            value,
				Properties:       protocolProperties,
			}
			err = eventpub.PublishEvent(ctx, common.PUBSUB_NAME, common.EVENT_POINT_RW_META_TOPIC, pointWriteDataEvent)
			if err != nil {
				common.Logger.Error("publishevent " + common.EVENT_POINT_RW_META_TOPIC + " error " + err.Error())
			}
			break
		}
	}
	if !success {
		err = errors.New("property not exist")
		return
	}
	if success { //设置成功，重新采集一次

		common.Logger.Debug(device.Identifier + " property " + propertyName + " set " + cast.ToString(value) + " success")
		err = startOneDeviceDataCollect(ctx, device)
		if err != nil {
			common.Logger.Error(err.Error())
			return
		}
	}
	return
}
