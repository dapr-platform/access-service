package eventsub

import (
	"access-service/service"
	"context"
	"encoding/json"
	"github.com/dapr/go-sdk/service/common"
	"log"
	"sync"
)

var subscribeMap = sync.Map{}

func NewPropertySetEventHandler(server common.Service, eventPub string, eventTopic string) {
	var sub = &common.Subscription{
		PubsubName: eventPub,
		Topic:      eventTopic,
		Route:      "/PropertySetEventHandler",
	}

	err := server.AddTopicEventHandler(sub, propertySetEventHandler)

	if err != nil {
		panic(err)
	}
}

func propertySetEventHandler(ctx context.Context, e *common.TopicEvent) (retry bool, err error) {
	log.Printf("propertySetEventHandler - PubsubName: %s, Topic: %s, ID: %s, \n", e.PubsubName, e.Topic, e.ID)
	var event = make(map[string]any, 0)
	err = json.Unmarshal(e.RawData, &event)
	if err != nil {
		log.Println("propertySetEventHandler - data ", string(e.RawData))
		log.Println("propertySetEventHandler - unmarshal error: ", err)
	} else {
		go func() {
			err = service.ProcessPropertySetMessage(context.Background(), event)
			if err != nil {
				log.Println("propertySetEventHandler - process error: ", err)
			}
		}()

	}

	return false, nil
}
