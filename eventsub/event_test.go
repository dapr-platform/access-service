package eventsub

import (
	"encoding/json"
	"testing"
)

func TestEvent(t *testing.T) {
	str := "{\"Payload\":{\"second\":1685323996},\"CurrentId\":\"f8f7c180-cc41-434c-8595-c41f281f719c\",\"Properties\":{\"devices\":[{\"id\":\"1903dd68d9b90b4afaf915e830969e85\",\"name\":\"水浸2\"},{\"id\":\"73f57a6f8668270f088ccb616d83f85b\",\"name\":\"水浸5\"},{\"id\":\"23c4f6f5269051c957e4fe02030e610b\",\"name\":\"水浸4\"},{\"id\":\"2aa3428edbc27005a8fdf1a95a7737b0\",\"name\":\"水浸1\"}],\"name\":\"设备采集\",\"product\":\"422sSMPtnq0iQH8eZckWD\",\"type\":\"DeviceDataCollection\"},\"IncomingBusiness\":{\"b089f4dc-90b2-4051-b034-060f781d9a77\":\"FlowLoop@output2\"},\"OutgoingBusiness\":{},\"SignalType\":\"SecondInterval\",\"PreNodeResult\":{\"595d347e-c83e-4fa7-afb1-a5536cf8d127\":{\"Id\":\"\",\"Valid\":true,\"Data\":null},\"b089f4dc-90b2-4051-b034-060f781d9a77\":{\"Id\":\"\",\"Valid\":true,\"Data\":null}}}"
	event := make(map[string]any, 0)
	err := json.Unmarshal([]byte(str), &event)
	if err != nil {
		t.Error(err)
	}
}
