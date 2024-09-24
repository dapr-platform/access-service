package protocol

import (
	"github.com/dapr-platform/common"
	"github.com/pkg/errors"
	"github.com/simonvetter/modbus"
	"testing"
	"time"
)

func TestAi(t *testing.T) {
	url := "tcp://182.92.117.41:40031"
	client, err := modbus.NewClient(&modbus.ClientConfiguration{
		URL:     url,
		Timeout: 1 * time.Second,
	})
	if err != nil {
		common.Logger.Error("modbus client error " + url + " " + err.Error())
		return
	}
	err = client.Open()
	if err != nil {
		common.Logger.Error("modbus client error " + url + " " + err.Error())
		t.Error(err)
		return
	}
	unit := uint8(1)
	mtype := "485"
	addr := uint16(0)
	quantity := uint16(2)
	err = client.SetUnitId(unit)
	if err != nil {
		err = errors.Wrap(err, "set unit id")
		return
	}
	var value any
	switch mtype {
	case "di":
		common.Logger.Debugf("%d di %d %d", unit, addr, quantity)
		value, err = client.ReadDiscreteInputs(addr, quantity)
	case "do":
		common.Logger.Debugf("%d do %d %d", unit, addr, quantity)
		value, err = client.ReadCoils(addr, quantity)
	case "485":
		common.Logger.Debugf("%d 485 %d %d", unit, addr, quantity)
		value, err = client.ReadRegisters(addr, quantity, modbus.HOLDING_REGISTER)
	case "ai":
		common.Logger.Debugf("%d ai %d %d", unit, addr, quantity)
		value, err = client.ReadRegisters(addr, quantity, modbus.INPUT_REGISTER)
	case "ao":
		common.Logger.Debugf("%d ao %d %d", unit, addr, quantity)
		value, err = client.ReadCoils(addr, quantity)
	}
	t.Log(value)
	if err != nil {
		t.Fatal(err)
	}

}
