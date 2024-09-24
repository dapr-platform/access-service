package protocol

import (
	"access-service/config"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/aldas/go-modbus-client/packet"
	"github.com/dapr-platform/common"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"net"
	"net/url"
	"sync"
	"time"
)

type RtuOverMqttClient struct {
	sync.Mutex
	Properties      map[string]any
	mqttClient      mqtt.Client
	conn            net.Conn
	mqttRunning     bool
	Protocol        string
	Status          int //0 closed ,1:open
	pubTopic        string
	processDataFunc func(data []byte)
}

func (m *RtuOverMqttClient) GetProperties() map[string]any {
	return m.Properties
}
func (m *RtuOverMqttClient) startMqttClient() (err error) {
	if m.mqttClient == nil {
		// 掉线重连
		var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
			fmt.Printf("Mqtt Connect lost: %v", err)
			i := 0
			for {
				time.Sleep(5 * time.Second)
				if !m.mqttClient.IsConnectionOpen() {
					i++
					fmt.Println("MQTT掉线重连...", i)
					if token := m.mqttClient.Connect(); token.Wait() && token.Error() != nil {
						fmt.Println("MQTT连接失败...重试", token.Error())
					} else {
						break
					}
				} else {
					m.subscribe()
					break
				}
			}
		}
		opts := mqtt.NewClientOptions()
		urlStr := cast.ToString(m.Properties["url"])
		info, err := url.Parse(urlStr)
		if err != nil {
			return errors.Wrap(err, "parse url error")
		}

		opts.SetUsername(info.User.Username())
		common.Logger.Debug("username=", info.User.Username())
		p, exist := info.User.Password()
		if exist {
			opts.SetPassword(p)
			common.Logger.Debug("password=", p)
		}

		opts.SetClientID("server_" + cast.ToString(m.Properties["identifier"]))
		common.Logger.Debug("client_id=server_", cast.ToString(m.Properties["identifier"]))
		opts.AddBroker(info.Host)
		common.Logger.Debug("MQTT CONNECT -- ", info.Host)
		opts.SetAutoReconnect(true)
		opts.SetOrderMatters(false)
		opts.OnConnectionLost = connectLostHandler
		opts.SetOnConnectHandler(func(c mqtt.Client) {
			if !m.mqttRunning {
				common.Logger.Debug("MQTT CONNECT SUCCESS -- ", cast.ToString(m.Properties["identifier"])+" "+info.Host)
			}
			m.Status = 1
			m.mqttRunning = true
		})

		m.mqttClient = mqtt.NewClient(opts)
		reconnec_number := 0
		for { // 失败重连
			if token := m.mqttClient.Connect(); token.Wait() && token.Error() != nil {
				reconnec_number++
				fmt.Println("MQTT连接失败...重试", reconnec_number, token.Error())
			} else {
				break
			}
			time.Sleep(5 * time.Second)
		}
		m.subscribe()
	}

	return
}
func (mc *RtuOverMqttClient) subscribe() {
	devicePubTopic := cast.ToString(mc.Properties["pub_topic"])

	if token := mc.mqttClient.Subscribe(devicePubTopic, byte(config.MQTT_QOS), func(c mqtt.Client, m mqtt.Message) {
		bytes, err := hex.DecodeString(string(m.Payload()))
		if err != nil {
			common.Logger.Error("decode mq msg payload error ", err)
			return
		}

		if mc.processDataFunc != nil {
			mc.processDataFunc(bytes)
		}
	}); token.Wait() &&
		token.Error() != nil {
		common.Logger.Error(token.Error())

	}
}

func (m *RtuOverMqttClient) Start() (err error) {

	m.pubTopic = cast.ToString(m.Properties["sub_topic"])
	m.startMqttClient()
	return
}
func (m *RtuOverMqttClient) GetStatus() int {
	return m.Status
}
func (m *RtuOverMqttClient) Stop() (err error) {
	m.Lock()
	defer m.Unlock()

	return
}

func (m *RtuOverMqttClient) ReadValue(properties map[string]any) (value any, err error) {
	if m.Status == 0 {
		err = errors.New("RtuOverMqttClient client is not ready")
		return
	}
	unit := cast.ToUint8(properties["unit"])
	addr := cast.ToUint16(properties["addr"])
	quantity := cast.ToUint16(properties["quantity"])
	if quantity == 0 {
		return
	}
	mtype := cast.ToString(properties["type"])
	if mtype == "" {
		err = errors.New("type is empty")
		return
	}
	funcCode := cast.ToUint8(properties["funcCode"])

	m.Lock()
	defer m.Unlock()

	var reqFuncCode uint8
	var rtuReq packet.Request
	var byteStr string
	switch mtype {
	case "di":
		common.Logger.Debugf("%d di %d %d", unit, addr, quantity)
		rtuReq = packet.ReadDiscreteInputsRequestRTU{
			ReadDiscreteInputsRequest: packet.ReadDiscreteInputsRequest{
				UnitID:       unit,
				StartAddress: addr,
				Quantity:     quantity,
			},
		}

	case "do":
		common.Logger.Debugf("%d do %d %d", unit, addr, quantity)
		rtuReq = packet.ReadCoilsRequestRTU{
			ReadCoilsRequest: packet.ReadCoilsRequest{
				UnitID:       unit,
				StartAddress: addr,
				Quantity:     quantity,
			},
		}
		byteStr = hex.EncodeToString(rtuReq.Bytes())
	case "485":
		common.Logger.Debugf("%d 485 %d %d", unit, addr, quantity)
		if funcCode != 0 {
			if funcCode == 0x03 {
				rtuReq = packet.ReadHoldingRegistersRequestRTU{
					ReadHoldingRegistersRequest: packet.ReadHoldingRegistersRequest{
						UnitID:       unit,
						StartAddress: addr,
						Quantity:     quantity,
					},
				}
			} else if funcCode == 0x04 {
				rtuReq = packet.ReadInputRegistersRequestRTU{
					ReadInputRegistersRequest: packet.ReadInputRegistersRequest{
						UnitID:       unit,
						StartAddress: addr,
						Quantity:     quantity,
					},
				}
			}
		} else {
			rtuReq = packet.ReadHoldingRegistersRequestRTU{
				ReadHoldingRegistersRequest: packet.ReadHoldingRegistersRequest{
					UnitID:       unit,
					StartAddress: addr,
					Quantity:     quantity,
				},
			}
		}

	case "ai":
		common.Logger.Debugf("%d ai %d %d", unit, addr, quantity)
		rtuReq = packet.ReadInputRegistersRequestRTU{
			ReadInputRegistersRequest: packet.ReadInputRegistersRequest{
				UnitID:       unit,
				StartAddress: addr,
				Quantity:     quantity,
			},
		}
	case "ao":
		common.Logger.Debugf("%d ao %d %d", unit, addr, quantity)
		rtuReq = packet.ReadCoilsRequestRTU{
			ReadCoilsRequest: packet.ReadCoilsRequest{
				UnitID:       unit,
				StartAddress: addr,
				Quantity:     quantity,
			},
		}
	}
	reqFuncCode = rtuReq.FunctionCode()
	byteStr = hex.EncodeToString(rtuReq.Bytes())
	common.Logger.Debug("mqtt send " + byteStr)
	m.mqttClient.Publish(m.pubTopic, byte(config.MQTT_QOS), false, byteStr)

	common.Logger.Debugf("unit %v  addr %v quantity %v mtype %v funcCode %v value=%v err=%v", unit, addr, quantity, mtype, funcCode, value, err)
	if err != nil {
		common.Logger.Error("err = ", err)
	}
	getStop := make(chan bool, 1)
	m.processDataFunc = func(data []byte) {
		common.Logger.Debug("mqtt receive data %x", data)
		/*
			hexData, err1 := hex.DecodeString(string(data)) //mqtt传的是16进制字符串。 dtu上设置的
			if err1 != nil {
				common.Logger.Error("decode string err = ", err1)
				return
			}*/

		resp, err1 := packet.ParseRTUResponse(data)
		if err1 != nil {
			common.Logger.Error("err = ", err1)
			return
		}
		if resp.FunctionCode() == reqFuncCode {
			switch reqFuncCode {
			case 0x02:
				value = resp.(*packet.ReadCoilsResponseRTU).Data
			case 0x03:
				value = resp.(*packet.ReadHoldingRegistersResponseRTU).Data
			case 0x04:
				value = resp.(*packet.ReadInputRegistersResponseRTU).Data
			default:
				common.Logger.Debug("no need funccode " + cast.ToString(reqFuncCode))

			}
			if value != nil {
				//
				if len(value.([]byte))%2 == 0 {
					value = BytesToUint16s(value.([]byte))
				}
			}
			getStop <- true
		}
	}
	defer func() { m.processDataFunc = nil }()
	select {
	case <-time.After(time.Second * 3):
		err = errors.New("mqtt request timed out")
		return
	case <-getStop:
		return

	}
	return
}
func BytesToUint16s(bytes []byte) []uint16 {
	numUint16s := len(bytes) / 2
	uint16s := make([]uint16, numUint16s)

	for i := 0; i < numUint16s; i++ {
		uint16s[i] = uint16(bytes[i*2])<<8 | uint16(bytes[i*2+1])
	}
	return uint16s
}

func (m *RtuOverMqttClient) WriteValue(properties map[string]any, value any) (err error) {
	if m.Status == 0 {
		err = errors.New("modbus client is not ready")
		return
	}
	unit := cast.ToUint8(properties["unit"])
	addr := cast.ToUint16(properties["addr"])
	//quantity := cast.ToUint16(properties["quantity"])
	mtype := cast.ToString(properties["type"])
	if mtype == "" {
		err = errors.New("type is empty")
		return
	}

	m.Lock()
	defer m.Unlock()
	data := make([]byte, 2)
	binary.BigEndian.PutUint16(data, cast.ToUint16(value))
	var byteStr string
	switch mtype {
	case "do":
		rtuReq := &packet.WriteSingleCoilRequestRTU{WriteSingleCoilRequest: packet.WriteSingleCoilRequest{
			UnitID:    unit,
			Address:   addr,
			CoilState: cast.ToInt(value) == 1,
		}}
		byteStr = hex.EncodeToString(rtuReq.Bytes())
	case "485":
		rtuReq := &packet.WriteMultipleRegistersRequestRTU{WriteMultipleRegistersRequest: packet.WriteMultipleRegistersRequest{
			UnitID:        unit,
			StartAddress:  addr,
			RegisterCount: 1,
			Data:          data,
		}}
		byteStr = hex.EncodeToString(rtuReq.Bytes())
	case "ao":

		rtuReq := &packet.WriteSingleRegisterRequestRTU{WriteSingleRegisterRequest: packet.WriteSingleRegisterRequest{
			UnitID:  unit,
			Address: addr,
			Data:    [2]byte{data[0], data[1]},
		}}
		byteStr = hex.EncodeToString(rtuReq.Bytes())
	}

	m.mqttClient.Publish(m.pubTopic, byte(config.MQTT_QOS), false, byteStr)
	common.Logger.Debugf("unit %v  addr %v  mtype %v  value=%v err=%v", unit, addr, mtype, value, err)

	if err != nil {
		common.Logger.Error("err = ", err)
	}
	return
}

func (m *RtuOverMqttClient) shouldReconnect(err error) (yes bool) {

	return
}
