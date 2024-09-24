package eventsub

import (
	mycommon "github.com/dapr-platform/common"
	"github.com/dapr/go-sdk/service/common"
)

func Sub(s common.Service) {
	NewPropertySetEventHandler(s, mycommon.PUBSUB_NAME, mycommon.PROPERTY_SET_TOPIC)
	NewCollectEventHandler(s, mycommon.PUBSUB_NAME, "device_collection")

}
