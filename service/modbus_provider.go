package service

import (
	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"strings"
)

type DeviceProviderProcessor interface {
	GetCustomPropertiesForProtocol(properties map[string]any) (prop map[string]any, err error)
	ProcessReadValue(readProperties map[string]any, value any) (val any, err error)
}

var DeviceProviderProcessorMap = make(map[string]DeviceProviderProcessor, 0)

func init() {
	DeviceProviderProcessorMap["IO"] = &IO_ModbusProvider{}
	DeviceProviderProcessorMap["485"] = &Common485_ModbusProvider{}
}
func GetDeviceProviderProcessor(accessType string) (DeviceProviderProcessor, error) {
	p, ok := DeviceProviderProcessorMap[accessType]
	if !ok {
		return nil, errors.New("modbus provider not found")
	}
	return p, nil
}

type IO_ModbusProvider struct {
}

func (m *IO_ModbusProvider) GetCustomPropertiesForProtocol(properties map[string]any) (prop map[string]any, err error) {
	prop = make(map[string]any, 0)

	unit, ok := properties["485地址"]
	if !ok {
		err = errors.New("485地址 not found")
		return
	}
	prop["unit"] = unit
	pointIo, ok := properties["端子"]
	if !ok {
		err = errors.New("端子 not found")
		return
	}
	addr, ok := properties["点位地址"]
	if ok {
		prop["addr"] = cast.ToInt(addr)
	} else {
		prop["addr"] = 0
	}

	prop["quantity"] = 4
	if strings.Index(cast.ToString(pointIo), "DI") == 0 {
		prop["type"] = "di"
	} else if strings.Index(cast.ToString(pointIo), "DO") == 0 {
		prop["type"] = "do"
	} else if strings.Index(cast.ToString(pointIo), "AI") == 0 {
		prop["type"] = "ai"
	} else if strings.Index(cast.ToString(pointIo), "AO") == 0 {
		prop["type"] = "ao"
	}
	prop["idx"] = cast.ToInt(cast.ToString(pointIo)[2:3]) - 1

	return
}
func (m *IO_ModbusProvider) ProcessReadValue(readProperties map[string]any, value any) (val any, err error) {
	idx := readProperties["idx"]
	mtype := readProperties["type"]
	switch mtype {
	case "di":
		fallthrough
	case "do":
		v := value.([]bool)
		val = cast.ToInt(v[cast.ToInt(idx)])
	case "ai":
		fallthrough
	case "ao":
		v := value.([]uint16)
		val = v[0]
	}

	return
}

type Common485_ModbusProvider struct {
}

func (m *Common485_ModbusProvider) GetCustomPropertiesForProtocol(properties map[string]any) (prop map[string]any, err error) {
	prop = make(map[string]any, 0)
	prop["type"] = "485"

	unit, ok := properties["485地址"]
	if !ok {
		err = errors.New("485地址 not found")
		return
	}
	prop["unit"] = unit
	pointAddr, ok := properties["点位地址"]
	if !ok {
		err = errors.New("点位地址 not found")
		return
	}
	prop["addr"] = cast.ToInt(pointAddr)
	length, ok := properties["长度"]
	if !ok {
		err = errors.New("长度 not found")
		return
	}
	prop["quantity"] = cast.ToInt(length)
	funcCode, ok := properties["功能码"]
	if ok {
		prop["funcCode"] = cast.ToInt(funcCode)
	}
	return
}
func (m *Common485_ModbusProvider) ProcessReadValue(readProperties map[string]any, value any) (val any, err error) {
	quantity := cast.ToInt(readProperties["quantity"])
	if quantity > 1 {
		val = value
	} else {
		val = value.([]uint16)[0]
	}

	return
}
