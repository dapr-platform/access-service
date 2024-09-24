package protocol

import (
	"access-service/eventpub"
	"context"
	"github.com/dapr-platform/common"
	"github.com/pkg/errors"
	"github.com/simonvetter/modbus"
	"github.com/spf13/cast"
	"sync"
	"time"
)

type ModbusClientWrapper struct {
	sync.Mutex
	Properties map[string]any
	Client     *modbus.ModbusClient
	Protocol   string
	Status     int //0 closed ,1:open
}

func (m *ModbusClientWrapper) GetProperties() map[string]any {
	return m.Properties
}
func (m *ModbusClientWrapper) Start() (err error) {
	for {

		if m.Status == 0 { //需要重联
			if m.Client != nil {
				m.Client.Close()
			}
			url := m.Protocol + "://" + cast.ToString(m.Properties["ip"]) + ":" + cast.ToString(m.Properties["port"])
			common.Logger.Debugf("start modbus client %s", url)
			common.Logger.Debugf("client connect %s", url)
			client, err := modbus.NewClient(&modbus.ClientConfiguration{
				URL:     url,
				Timeout: 5 * time.Second,
			})
			if err != nil {
				common.Logger.Error("modbus client error " + url + " " + err.Error())
				time.Sleep(time.Second * 5)
				continue
			}
			common.Logger.Debugf("before open %s", url)
			m.Lock()
			err = client.Open()
			common.Logger.Debugf("end open %s", url)
			if err != nil {
				common.Logger.Error("modbus client error " + url + " " + err.Error())
				eventpub.ConstructAndSendEvent(context.Background(), common.EventTypeDevice, url+" 连接失败", err.Error(), common.EventStatusActive, common.EventLevelCritical, time.Now(), m.Properties["ip"].(string), m.Properties["port"].(string), "")
				time.Sleep(time.Second * 5)
				m.Unlock()
				continue
			}
			m.Status = 1
			m.Client = client
			m.Unlock()
			eventpub.ConstructAndSendEvent(context.Background(), common.EventTypeDevice, url+" 连接失败", "", common.EventStatusClosed, common.EventLevelCritical, time.Now(), m.Properties["ip"].(string), m.Properties["port"].(string), "")

		}

		time.Sleep(time.Second * 5)
	}

	return
}
func (m *ModbusClientWrapper) GetStatus() int {
	return m.Status
}
func (m *ModbusClientWrapper) Stop() (err error) {
	m.Lock()
	defer m.Unlock()
	if m.Client != nil {
		return m.Client.Close()
	}
	return
}

func (m *ModbusClientWrapper) ReadValue(properties map[string]any) (value any, err error) {
	if m.Status == 0 {
		err = errors.New("modbus client is not ready")
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
	if m.Client == nil {
		err = errors.New("modbus client is nil")
		return
	}
	err = m.Client.SetUnitId(unit)
	if err != nil {
		err = errors.Wrap(err, "set unit id")
		return
	}
	switch mtype {
	case "di":
		common.Logger.Debugf("%d di %d %d", unit, addr, quantity)
		value, err = m.Client.ReadDiscreteInputs(addr, quantity)
	case "do":
		common.Logger.Debugf("%d do %d %d", unit, addr, quantity)
		value, err = m.Client.ReadCoils(addr, quantity)
	case "485":
		common.Logger.Debugf("%d 485 %d %d", unit, addr, quantity)
		if funcCode != 0 {
			if funcCode == 0x03 {
				value, err = m.Client.ReadRegisters(addr, quantity, modbus.HOLDING_REGISTER)
			} else if funcCode == 0x04 {
				value, err = m.Client.ReadRegisters(addr, quantity, modbus.INPUT_REGISTER)
			}
		} else {
			value, err = m.Client.ReadRegisters(addr, quantity, modbus.HOLDING_REGISTER)
		}

	case "ai":
		common.Logger.Debugf("%d ai %d %d", unit, addr, quantity)
		value, err = m.Client.ReadRegisters(addr, quantity, modbus.INPUT_REGISTER)
	case "ao":
		common.Logger.Debugf("%d ao %d %d", unit, addr, quantity)
		value, err = m.Client.ReadCoils(addr, quantity)
	}
	common.Logger.Debugf("unit %v  addr %v quantity %v mtype %v funcCode %v value=%v err=%v", unit, addr, quantity, mtype, funcCode, value, err)
	if err != nil {
		common.Logger.Error("err = ", err)
		if m.shouldReconnect(err) {
			m.Status = 0
		}
	}
	/*
		if err == nil {
			m.Status = 1
		} else if err == modbus.ErrRequestTimedOut {

		} else {
			log.Println("err = ", err)
			m.Client = nil
			m.Status = 0
		}

	*/

	return
}

func (m *ModbusClientWrapper) WriteValue(properties map[string]any, value any) (err error) {
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
	if m.Client == nil {
		err = errors.New("modbus client is nil")
		return
	}
	err = m.Client.SetUnitId(unit)
	if err != nil {
		err = errors.Wrap(err, "set unit id")
		return
	}
	switch mtype {
	case "do":
		idx, exist := properties["idx"]
		if !exist {
			err = errors.New("idx is empty")
			return
		}
		err = m.Client.WriteCoil(addr+cast.ToUint16(idx), cast.ToBool(value))
	case "485":
		err = m.Client.WriteRegister(addr, cast.ToUint16(value))
	case "ao":
		err = m.Client.WriteRegister(addr, cast.ToUint16(value))
	}
	common.Logger.Debugf("unit %v  addr %v  mtype %v  value=%v err=%v", unit, addr, mtype, value, err)

	if err != nil {
		common.Logger.Error("err = ", err)
		if m.shouldReconnect(err) {
			m.Status = 0
		}
	}
	/*
		if err == nil {
			m.Status = 1
		} else {
			log.Println("err = ", err)
			m.Client = nil
			m.Status = 0
		}

			if err == modbus.ErrRequestTimedOut {
				m.Client = nil
				m.Status = 0
			}

	*/
	return
}

func (m *ModbusClientWrapper) shouldReconnect(err error) (yes bool) {
	yes = true

	if err == nil ||
		err.Error() == modbus.ErrRequestTimedOut.Error() ||
		err.Error() == modbus.ErrProtocolError.Error() ||
		err.Error() == modbus.ErrIllegalFunction.Error() ||
		err.Error() == modbus.ErrIllegalDataAddress.Error() ||
		err.Error() == modbus.ErrIllegalDataValue.Error() ||
		err.Error() == modbus.ErrServerDeviceFailure.Error() ||
		err.Error() == modbus.ErrMemoryParityError.Error() ||
		err.Error() == modbus.ErrServerDeviceBusy.Error() ||
		err.Error() == modbus.ErrGWPathUnavailable.Error() ||
		err.Error() == modbus.ErrGWTargetFailedToRespond.Error() {
		yes = false
	}

	return
}
