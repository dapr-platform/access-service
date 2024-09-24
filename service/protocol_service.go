package service

import (
	"access-service/model"
	"access-service/protocol"
	"context"
	"encoding/json"
	"github.com/dapr-platform/common"
	"github.com/pkg/errors"
	"sync"
	"time"
)

var protocolProcesserMap = make(map[string]protocol.ProtocolProcessor, 0)
var modbusClientMapLock sync.Mutex

func init() {
	go refreshAllProtocols()
}
func refreshAllProtocols() {

	time.Sleep(time.Second * 5)
	for {
		protocols, err := common.DbQuery[model.Access_protocol](context.Background(), common.GetDaprClient(), model.Access_protocolTableInfo.Name, "")
		if err != nil {
			common.Logger.Error(err)
		} else {
			for _, one := range protocols {
				properties := make(map[string]any, 0)
				err = json.Unmarshal([]byte(one.Properties), &properties)
				if err != nil {
					common.Logger.Error(errors.Wrap(err, "unmarshal protocol properties"))
					continue
				}
				switch one.Type {
				case "modbus_tcp":

					p, exist := protocolProcesserMap[one.Identifier]
					if exist {
						if p.GetProperties()["ip"] != properties["ip"] || p.GetProperties()["port"] != properties["port"] {
							p.Stop()
							common.Logger.Debugf("%s change ip and port , then restart")
						} else {
							continue
						}
					}
					err = startModbusTcpClient(one.Identifier, properties)
					if err != nil {
						common.Logger.Error(errors.Wrap(err, "startModbusTcpService "+one.Identifier+" error "))
						continue
					}
				case "rtu_over_mqtt":
					p, exist := protocolProcesserMap[one.Identifier]
					if exist {
						if p.GetProperties()["url"] != properties["url"] || p.GetProperties()["port"] != properties["port"] || p.GetProperties()["pub_topic"] != properties["pub_topic"] || p.GetProperties()["sub_topic"] != properties["sub_topic"] {
							p.Stop()
							common.Logger.Debugf("%s change info , then restart")
						} else {
							continue
						}
					}
					err = startModbusRtuOverMqttClient(one.Identifier, one.Type, properties)
				default:
					common.Logger.Error("TODO " + one.Type)

				}

			}
		}
		time.Sleep(time.Second * 60)
	}
}
func startModbusRtuOverMqttClient(identifier string, ptype string, properties map[string]any) (err error) {
	modbusClientMapLock.Lock()
	defer modbusClientMapLock.Unlock()
	if _, ok := properties["url"]; !ok {
		common.Logger.Error("url not found")
		err = errors.New("url not found")
		return
	}
	if _, ok := properties["pub_topic"]; !ok {
		common.Logger.Error("pub_topic not found")
		err = errors.New("pub_topic not found")
		return
	}
	if _, ok := properties["sub_topic"]; !ok {
		common.Logger.Error("sub_topic not found")
		err = errors.New("sub_topic not found")
		return
	}
	properties["identifier"] = identifier
	client := &protocol.RtuOverMqttClient{
		Properties: properties,
		Protocol:   ptype,
	}
	protocolProcesserMap[identifier] = client
	go client.Start()
	return
}

func startModbusTcpClient(identifier string, properties map[string]any) (err error) {

	modbusClientMapLock.Lock()
	defer modbusClientMapLock.Unlock()

	_, ok := properties["ip"]
	if !ok {
		common.Logger.Error("ip not found")
		err = errors.New("ip not found")
		return
	}
	_, ok = properties["port"]
	if !ok {
		common.Logger.Error("port not found")
		err = errors.New("port not found")
		return
	}
	client := &protocol.ModbusClientWrapper{
		Properties: properties,
		Protocol:   "tcp",
	}
	protocolProcesserMap[identifier] = client
	go client.Start()

	return
}

func GetProtocolStatus(identifier string) (status int, err error) {
	if !CheckProtocolExists(identifier) {
		return 0, errors.New("protocol " + identifier + " not exists")
	} else {
		p := protocolProcesserMap[identifier]
		status = p.GetStatus()
	}
	return
}
func CheckProtocolExists(identifier string) bool {
	_, ok := protocolProcesserMap[identifier]
	return ok
}

func ProtocolReadValue(identifier string, properties map[string]any) (value any, err error) {
	if !CheckProtocolExists(identifier) {
		err = errors.New("protocol processor not exists")
		return
	}
	//TODO switch protocol to process
	processor := protocolProcesserMap[identifier]
	return processor.ReadValue(properties)
}

func ProtocolWriteValue(identifier string, properties map[string]any, value any) (err error) {
	if !CheckProtocolExists(identifier) {
		err = errors.New("protocol processor not exists")
		return
	}
	processor := protocolProcesserMap[identifier]
	return processor.WriteValue(properties, value)
}
