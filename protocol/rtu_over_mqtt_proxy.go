package protocol

import (
	"access-service/config"
	"encoding/hex"
	"fmt"
	"github.com/dapr-platform/common"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/pkg/errors"
	"github.com/simonvetter/modbus"
	"github.com/spf13/cast"
	"io"
	"net"
	"net/url"
	"sync"
	"time"
)

type Error string

const (
	maxRTUFrameLength int = 256
	// coils
	fcReadCoils          uint8 = 0x01
	fcWriteSingleCoil    uint8 = 0x05
	fcWriteMultipleCoils uint8 = 0x0f

	// discrete inputs
	fcReadDiscreteInputs uint8 = 0x02

	// 16-bit input/holding registers
	fcReadHoldingRegisters       uint8 = 0x03
	fcReadInputRegisters         uint8 = 0x04
	fcWriteSingleRegister        uint8 = 0x06
	fcWriteMultipleRegisters     uint8 = 0x10
	fcMaskWriteRegister          uint8 = 0x16
	fcReadWriteMultipleRegisters uint8 = 0x17
	fcReadFifoQueue              uint8 = 0x18

	// file access
	fcReadFileRecord  uint8 = 0x14
	fcWriteFileRecord uint8 = 0x15

	// exception codes
	exIllegalFunction         uint8 = 0x01
	exIllegalDataAddress      uint8 = 0x02
	exIllegalDataValue        uint8 = 0x03
	exServerDeviceFailure     uint8 = 0x04
	exAcknowledge             uint8 = 0x05
	exServerDeviceBusy        uint8 = 0x06
	exMemoryParityError       uint8 = 0x08
	exGWPathUnavailable       uint8 = 0x0a
	exGWTargetFailedToRespond uint8 = 0x0b

	// errors
	ErrConfigurationError      Error = "configuration error"
	ErrRequestTimedOut         Error = "request timed out"
	ErrIllegalFunction         Error = "illegal function"
	ErrIllegalDataAddress      Error = "illegal data address"
	ErrIllegalDataValue        Error = "illegal data value"
	ErrServerDeviceFailure     Error = "server device failure"
	ErrAcknowledge             Error = "request acknowledged"
	ErrServerDeviceBusy        Error = "server device busy"
	ErrMemoryParityError       Error = "memory parity error"
	ErrGWPathUnavailable       Error = "gateway path unavailable"
	ErrGWTargetFailedToRespond Error = "gateway target device failed to respond"
	ErrBadCRC                  Error = "bad crc"
	ErrShortFrame              Error = "short frame"
	ErrProtocolError           Error = "protocol error"
	ErrBadUnitId               Error = "bad unit id"
	ErrBadTransactionId        Error = "bad transaction id"
	ErrUnknownProtocolId       Error = "unknown protocol identifier"
	ErrUnexpectedParameters    Error = "unexpected parameters"
)

type RtuOverMqttClientProxy struct {
	sync.Mutex
	Properties  map[string]any
	Client      *ModbusClientWrapper
	mqttClient  mqtt.Client
	conn        net.Conn
	mqttRunning bool
	Protocol    string
	Status      int //0 closed ,1:open
	pubTopic    string
}

func (m *RtuOverMqttClientProxy) GetProperties() map[string]any {
	return m.Properties
}
func (m *RtuOverMqttClientProxy) startMqttClient() (err error) {
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
		p, exist := info.User.Password()
		if exist {
			opts.SetPassword(p)
		}

		opts.SetClientID(cast.ToString(m.Properties["identifier"]))
		opts.AddBroker(info.Host + ":" + info.Port())
		opts.SetAutoReconnect(true)
		opts.SetOrderMatters(false)
		opts.OnConnectionLost = connectLostHandler
		opts.SetOnConnectHandler(func(c mqtt.Client) {
			if !m.mqttRunning {
				common.Logger.Debug("MQTT CONNECT SUCCESS -- ", cast.ToString(m.Properties["identifier"])+" "+info.Host+":"+info.Port())
			}
			m.mqttRunning = true
		})

		m.mqttClient = mqtt.NewClient(opts)
		reconnec_number := 0
		for { // 失败重连
			if token := m.mqttClient.Connect(); token.Wait() && token.Error() != nil {
				reconnec_number++
				fmt.Println("MQTT连接失败...重试", reconnec_number)
			} else {
				break
			}
			time.Sleep(5 * time.Second)
		}
		m.subscribe()
	}

	return
}
func (mc *RtuOverMqttClientProxy) subscribe() {
	if token := mc.mqttClient.Subscribe(config.MQTT_DEVICE_TOPIC, byte(config.MQTT_QOS), func(c mqtt.Client, m mqtt.Message) {
		bytes, err := hex.DecodeString(string(m.Payload()))
		if err != nil {
			common.Logger.Error("decode mq msg payload error ", err)
			return
		}
		if mc.conn != nil {
			n, err := mc.conn.Write(bytes)
			if err != nil {
				common.Logger.Error("write internal conn error ", err)
				return
			}
			if n != len(bytes) {
				common.Logger.Error("write internal conn error ", "write n != len")
			}
		}
	}); token.Wait() &&
		token.Error() != nil {
		common.Logger.Error(token.Error())

	}
}
func (m *RtuOverMqttClientProxy) startSocketProxy(port string) (err error) {

	listener, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		return errors.Wrap(err, "bind error")
	}
	defer listener.Close()
	common.Logger.Info("bind", "127.0.0.1:"+port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			common.Logger.Error(err)
		} else {
			m.connectionHandler(conn)
		}
	}
}
func (m *RtuOverMqttClientProxy) connectionHandler(conn net.Conn) {
	m.conn = conn
	defer conn.Close()
	for {
		data, err := m.readRTUFrame(m.conn)
		if err != nil {
			common.Logger.Error("read buffer error")
			continue
		}
		if m.mqttRunning {
			m.mqttClient.Publish(m.pubTopic, byte(config.MQTT_QOS), false, data)
		} else {
			common.Logger.Error("mqtt client is not running")
		}
	}
}

func (m *RtuOverMqttClientProxy) readRTUFrame(conn net.Conn) (res []byte, err error) {
	var rxbuf []byte
	var byteCount int
	var bytesNeeded int

	rxbuf = make([]byte, 2048)

	// read the serial ADU header: unit id (1 byte), function code (1 byte) and
	// PDU length/exception code (1 byte)
	byteCount, err = io.ReadFull(conn, rxbuf[0:3])
	if (byteCount > 0 || err == nil) && byteCount != 3 {
		err = modbus.ErrShortFrame
		return
	}
	if err != nil && err != io.ErrUnexpectedEOF {
		return
	}

	// figure out how many further bytes to read
	bytesNeeded, err = m.expectedResponseLenth(uint8(rxbuf[1]), uint8(rxbuf[2]))
	if err != nil {
		return
	}

	// we need to read 2 additional bytes of CRC after the payload
	bytesNeeded += 2

	// never read more than the max allowed frame length
	if byteCount+bytesNeeded > maxRTUFrameLength {
		err = modbus.ErrProtocolError
		return
	}

	byteCount, err = io.ReadFull(conn, rxbuf[3:3+bytesNeeded])
	if err != nil && err != io.ErrUnexpectedEOF {
		return
	}
	if byteCount != bytesNeeded {
		common.Logger.Error("expected %v bytes, received %v", bytesNeeded, byteCount)
		err = modbus.ErrShortFrame
		return
	}

	res = rxbuf[0 : 3+bytesNeeded]
	return
}
func (m *RtuOverMqttClientProxy) expectedResponseLenth(responseCode uint8, responseLength uint8) (byteCount int, err error) {
	switch responseCode {
	case fcReadHoldingRegisters,
		fcReadInputRegisters,
		fcReadCoils,
		fcReadDiscreteInputs:
		byteCount = int(responseLength)
	case fcWriteSingleRegister,
		fcWriteMultipleRegisters,
		fcWriteSingleCoil,
		fcWriteMultipleCoils:
		byteCount = 3
	case fcMaskWriteRegister:
		byteCount = 5
	case fcReadHoldingRegisters | 0x80,
		fcReadInputRegisters | 0x80,
		fcReadCoils | 0x80,
		fcReadDiscreteInputs | 0x80,
		fcWriteSingleRegister | 0x80,
		fcWriteMultipleRegisters | 0x80,
		fcWriteSingleCoil | 0x80,
		fcWriteMultipleCoils | 0x80,
		fcMaskWriteRegister | 0x80:
		byteCount = 0
	default:
		err = modbus.ErrProtocolError
	}

	return
}

func (m *RtuOverMqttClientProxy) Start() (err error) {
	port := cast.ToString(m.Properties["port"])
	if port == "" {
		return errors.New("port is empty")
	}
	m.pubTopic = cast.ToString(m.Properties["topic"])
	go m.startSocketProxy(port)
	m.startMqttClient()
	m.Properties["ip"] = "127.0.0.1"
	m.Client = &ModbusClientWrapper{
		Properties: m.Properties,
		Protocol:   "rtuovertcp",
	}
	go m.Client.Start()

	return
}
func (m *RtuOverMqttClientProxy) GetStatus() int {
	return m.Status
}
func (m *RtuOverMqttClientProxy) Stop() (err error) {
	m.Lock()
	defer m.Unlock()
	if m.Client != nil {
		m.Client.Stop()
	}

	return
}

func (m *RtuOverMqttClientProxy) ReadValue(properties map[string]any) (value any, err error) {
	if m.Status == 0 {
		err = errors.New("modbus client is not ready")
		return
	}
	return m.Client.ReadValue(properties)
}

func (m *RtuOverMqttClientProxy) WriteValue(properties map[string]any, value any) (err error) {
	if m.Status == 0 {
		err = errors.New("modbus client is not ready")
		return
	}
	return m.Client.WriteValue(properties, value)
}

func (m *RtuOverMqttClientProxy) shouldReconnect(err error) (yes bool) {

	return
}
