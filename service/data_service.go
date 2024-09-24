package service

import (
	"access-service/entity"
	"access-service/eventpub"
	"context"
	"encoding/json"
	"fmt"
	"github.com/dapr-platform/common"
	"github.com/pkg/errors"
	"strings"
	"time"
)

/*
func init() {
	p, _ := ants.NewPool(100)
	MqttListen(config.MQTT_BROKER, config.MQTT_USER, config.MQTT_PASSWORD, config.MQTT_CLIENT_ID, func(c mqtt.Client, m mqtt.Message) {
		_ = p.Submit(func() { //deviceProcess
			//GetDiscoveryService().MqttDiscoveryDevice(m.Topic(), m.Payload())
			MqttProcessDataDevice(m.Topic(), m.Payload())
		})
	}, func(c mqtt.Client, m mqtt.Message) {
		_ = p.Submit(func() { //gatewayProcess
			//GetDiscoveryService().MqttDiscoveryGateway(m.Topic(), m.Payload())
			MqttProcessDataGateway(m.Topic(), m.Payload())
		})
	})
}

*/

func MqttProcessDataDevice(topic string, payload []byte) (err error) {
	return
}

func GetHistoryData(ctx context.Context, ids, startTime, endTime string) (data []entity.DeviceHistoryData, err error) {
	selectStrFmt := `d.device_id as id,d.device_name as name,
        json_agg(
            json_build_object(
                'id', d.point_id,
                'name', d.point_name,
                'datas', (SELECT 
                                json_agg(
                                    json_build_object(
                                        'ts', f.ts,
                                        'value', f.value
                                    ) ORDER BY f.ts
                                ) 
                            FROM 
                                f_point_data f 
                            WHERE 
                                f.id = d.point_id 
                                AND f.ts >= '%s' 
                                AND f.ts <= '%s'
            )
        )
			) as points
`
	selectStr := fmt.Sprintf(selectStrFmt, startTime, endTime)
	idSlice := strings.Split(ids, ",")
	for i, id := range idSlice {
		idSlice[i] = fmt.Sprintf("'%s'", id)
	}
	newIds := strings.Join(idSlice, ",")
	whereStr := `1=1 GROUP BY 
    d.device_id,
    d.device_name`
	fromStrFmt := `(
        SELECT 
            d.id AS device_id,
            d.name AS device_name,
            p.id AS point_id,
            p.name AS point_name
        FROM 
            o_device d
            INNER JOIN o_point p ON d.id = p.device_id
        WHERE 
            d.id in (%s)
    ) AS d
`
	fromStr := fmt.Sprintf(fromStrFmt, newIds)
	data, err = common.CustomSql[entity.DeviceHistoryData](ctx, common.GetDaprClient(), selectStr, fromStr, whereStr)
	if err != nil {
		err = errors.Wrap(err, "customSql query history error . select "+selectStr+" from "+fromStr+" where "+whereStr)
	}
	return
}

func MqttProcessDataGateway(topic string, payload []byte) (err error) {
	arr := strings.Split(topic, "/")
	if len(arr) != 4 {
		common.Logger.Error("MqttProcessData topic format is not gateway type " + topic)
		return
	}
	provider := arr[2]
	deviceId := arr[3]
	processor, exists := GetProvider(provider)
	if !exists {
		//common.Logger.Warning("MqttProcessDataGateway no provider for gateway type " + topic)
		processor, exists = GetProvider("default")
		if !exists {
			common.Logger.Error("MqttProcessDataGateway no default provider for gateway type " + topic)
			return
		}

	}
	deviceDataInfos, err := processor.ProcessDataByGatewayPayload(deviceId, payload)
	if err != nil {
		common.Logger.Error("MqttProcessDataGateway error ", err)
		return
	}

	err = ProcessDeviceDataInfo(context.Background(), deviceDataInfos)
	return
}

func ProcessDeviceDataInfo(ctx context.Context, deviceDataInfos []entity.DeviceDataInfo) (err error) {

	//err = common.DbBatchUpsert[model.Point_data](context.Background(), common.GetDaprClient(),datas,model.Point_dataTableInfo.Identifier,model.Point_data_FIELD_NAME_id+","+model.Point_data_FIELD_NAME_ts+","+model.Point_data_FIELD_NAME_key)
	/*
		datas := make([]model.Point_data, 0)
		for _, deviceDataInfo := range deviceDataInfos {
			for _, pointData := range deviceDataInfo.Points {
				datas = append(datas, pointData)
			}
		}
		err = common.DbBatchInsert[model.Point_data](ctx, common.GetDaprClient(), datas, model.Point_dataTableInfo.Name)
		if err != nil {
			common.Logger.Error("ProcessDataByGatewayPayload DbBatchInsert error ", err)
			return
		}


	*/
	for _, deviceDataInfo := range deviceDataInfos {

		deviceInfoMsg := entity.DeviceInfoMsg{
			Identifier: deviceDataInfo.DeviceIdentifier,
			Ts:         time.Now().UnixMilli(),
			Properties: make(map[string]any, 0),
		}
		for _, p := range deviceDataInfo.Points {
			deviceInfoMsg.Properties[p.Key] = p.Value
		}

		err = eventpub.PublishEvent(ctx, common.PUBSUB_NAME, common.DEVICE_DATA_TOPIC, deviceInfoMsg)
		if err != nil {
			common.Logger.Error("PublishEvent error ", err)
			return
		}
		s, _ := json.Marshal(deviceInfoMsg)
		common.Logger.Debug("PublishEvent success ", string(s))
	}
	return
}
