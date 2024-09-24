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
	"time"
)

func StartDataCollect(event entity.DataCollectEvent) (err error) {
	switch event.DataType {
	case "device":
		err = startDevicesDataCollect(context.Background(), event.Devices)
	case "tag":
		err = startDataCollectWithTags(context.Background(), event.Product, event.Value)
	case "product":
		err = startDataCollectWithProductId(context.Background(), event.Product)

	}
	return
}
func startDataCollectWithProductId(ctx context.Context, productId string) (err error) {
	qstr := "product_id=" + productId + ""
	devices, err := common.DbQuery[entity.DeviceWithPoints](ctx, common.GetDaprClient(), model.Device_with_pointsTableInfo.Name, qstr)
	if err != nil {
		err = errors.Wrap(err, "select data error")
		return
	}
	for _, device := range devices {
		dev := device
		go func() {
			err = startOneDeviceDataCollect(ctx, &dev)
			if err != nil {
				common.Logger.Error(err.Error())
				return
			}
		}()

	}
	return
}

func startDataCollectWithTags(ctx context.Context, productId string, tags string) (err error) {
	selectStr := "*"
	fromStr := model.Device_with_pointsTableInfo.Name
	whereStr := ""
	if tags != "" {
		arr := strings.Split(tags, ",")
		for _, s := range arr {
			if s != "" {
				whereStr += " '" + s + "'=any(tags)" + " and"
			}

		}
	}
	if productId != "" {
		whereStr += " product_id='" + productId + "' and"
	}

	if whereStr != "" {
		whereStr = whereStr[:strings.LastIndex(whereStr, " and")]
	} else {
		whereStr = "1=1"
	}

	devices, err := common.CustomSql[entity.DeviceWithPoints](ctx, common.GetDaprClient(), selectStr, fromStr, whereStr)
	if err != nil {
		err = errors.Wrap(err, "select data error")
		return
	}
	for _, device := range devices {
		dev := device
		go func() {
			err = startOneDeviceDataCollect(ctx, &dev)
			if err != nil {
				common.Logger.Error(err.Error())
				return
			}
		}()
	}
	return
}

func startDevicesDataCollect(ctx context.Context, devices []entity.DataCollectEventDevice) (err error) {
	for _, device := range devices {
		deviceWithPoints, err := common.DbGetOne[entity.DeviceWithPoints](context.Background(), common.GetDaprClient(), model.Device_with_pointsTableInfo.Name, model.Device_with_points_FIELD_NAME_id+"="+device.Id)
		if err != nil {
			err = errors.Wrap(err, "get device error")
			common.Logger.Error(err.Error())
			continue
		}
		if deviceWithPoints == nil {
			common.Logger.Error("device " + device.Id + " not found")
			continue
		}
		go func() {
			err1 := startOneDeviceDataCollect(ctx, deviceWithPoints)
			if err1 != nil {
				common.Logger.Error(err1.Error())
				return
			}
		}()

	}
	return
}
func startGatewayDataCollect(ctx context.Context, device *entity.DeviceWithPoints) (err error) {
	protocolIdentifierStr := ""
	for _, t := range device.Tags {
		if strings.Index(t, "接入协议") == 0 {
			protocolIdentifierStr = strings.Split(t, ":")[1]
		}
	}
	protoIdentifiers := strings.Split(protocolIdentifierStr, ",")
	allStatus := 0
	for _, pi := range protoIdentifiers {
		status, err1 := GetProtocolStatus(pi)
		if err1 != nil {
			common.Logger.Error(err1)
			eventpub.ConstructAndSendEvent(context.Background(), common.EventTypeDevice, device.Identifier+" 连接点"+pi+"出错", err1.Error(), common.EventStatusActive, common.EventLevelMajor, time.Now(), pi, pi, "")
			continue
		} else {
			eventpub.ConstructAndSendEvent(context.Background(), common.EventTypeDevice, device.Identifier+" 连接点"+pi+"出错", "", common.EventStatusClosed, common.EventLevelMajor, time.Now(), pi, pi, "")

		}
		if allStatus == 0 {
			allStatus = status
		}

	}
	deviceDataInfo := entity.DeviceDataInfo{
		DeviceIdentifier: device.Identifier,
		Points:           make([]entity.PointData, 0),
	}
	deviceDataInfo.Points = append(deviceDataInfo.Points, entity.PointData{
		ID:    common.GetMD5Hash(device.Identifier + "_" + "运行状态"),
		Ts:    common.LocalTime(time.Now()),
		Key:   "运行状态",
		Value: allStatus,
	})

	err = ProcessDeviceDataInfo(ctx, []entity.DeviceDataInfo{deviceDataInfo})
	return
}

func startOneDeviceDataCollect(ctx context.Context, device *entity.DeviceWithPoints) (err error) {
	common.Logger.Debug("startOneDeviceDataCollect " + device.Identifier)
	if device.Enabled == 0 {
		common.Logger.Debug("device " + device.Identifier + " is disabled")
		//eventpub.ConstructAndSendEvent(context.Background(), common.EventTypeDevice, device.Identifier+" 禁用状态", device.Identifier+" 禁用状态", common.EventStatusActive, common.EventLevelWarning, time.Now(), device.Identifier, device.Identifier, "")
		return
	}
	if device.Type == 2 { //网关
		err = startGatewayDataCollect(ctx, device)
		return
	}

	deviceDataInfo := entity.DeviceDataInfo{
		DeviceIdentifier: device.Identifier,
		Points:           make([]entity.PointData, 0),
	}
	ts := common.LocalTime(time.Now())
	for _, point := range device.Points {
		if point.Id == "" {
			continue
		}
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
			common.Logger.Error(device.Identifier + " " + point.Identifier + " has no protocol")
			continue
		}
		accessType, ok := properties["接入类型"] //接入类型限定了 接入协议和解析方式
		if !ok {
			common.Logger.Error(device.Identifier + " " + point.Identifier + " has no access type")
			continue
		}
		deviceProcessor, err1 := GetDeviceProviderProcessor(cast.ToString(accessType))
		if err1 != nil {
			common.Logger.Error(device.Identifier + " " + point.Identifier + " GetDeviceProviderProcessor " + err1.Error())
			continue
		}
		readProperties, err2 := deviceProcessor.GetCustomPropertiesForProtocol(properties)
		if err2 != nil {
			//如果485不配置全，那么可能是另外的point采集全部的，然后再解析
			//common.Logger.Error(device.Identifier + " " + point.Identifier + " getReadProperties " + err.Error())
			continue
		}
		readVal, err3 := ProtocolReadValue(cast.ToString(protocolId), readProperties)
		if err3 != nil {
			common.Logger.Error(device.Identifier + " " + point.Identifier + " ProtocolReadValue " + err3.Error())
			continue
		}
		if readVal == nil {
			common.Logger.Error(device.Identifier + " " + point.Identifier + " ProtocolReadValue return nil")
			continue
		}

		val, err4 := deviceProcessor.ProcessReadValue(readProperties, readVal)
		if err4 != nil {
			common.Logger.Error(device.Identifier + " " + point.Identifier + " processReadValue " + err4.Error())
			continue
		}
		pointData := entity.PointData{
			ID:    common.GetMD5Hash(device.Identifier + "_" + point.Identifier + "_" + ts.String()),
			Ts:    ts,
			Key:   point.Identifier,
			Value: val,
		}

		pointReadDataEvent := entity.PointRWDataMetaInfo{
			Type:             "r",
			DeviceIdentifier: device.Identifier,
			PointIdentifier:  point.Identifier,
			Value:            val,
			Properties:       readProperties,
		}
		err = eventpub.PublishEvent(ctx, common.PUBSUB_NAME, common.EVENT_POINT_RW_META_TOPIC, pointReadDataEvent)
		if err != nil {
			common.Logger.Error(device.Identifier + " " + point.Identifier + " publishEvent " + err.Error())
		}
		deviceDataInfo.Points = append(deviceDataInfo.Points, pointData)
	}
	if len(deviceDataInfo.Points) == 0 { // 没有取到数据，也向上发， things-service 收到没有数据的，设置状态
		common.Logger.Debug(deviceDataInfo.DeviceIdentifier + " " + "no data to process")
		//return
	}
	common.Logger.Debug("ProcessDeviceDataInfo " + deviceDataInfo.DeviceIdentifier)
	err = ProcessDeviceDataInfo(ctx, []entity.DeviceDataInfo{deviceDataInfo})
	return
}
