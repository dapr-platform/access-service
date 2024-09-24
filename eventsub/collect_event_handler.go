package eventsub

import (
	"access-service/entity"
	"access-service/service"
	"context"
	"encoding/json"
	"github.com/dapr/go-sdk/service/common"
	"log"
)

func NewCollectEventHandler(server common.Service, eventPub string, eventTopic string) {
	var sub = &common.Subscription{
		PubsubName: eventPub,
		Topic:      eventTopic,
		Route:      "/CollectEventHandler",
	}

	err := server.AddTopicEventHandler(sub, collectEventHandler)

	if err != nil {
		panic(err)
	}
}

func collectEventHandler(ctx context.Context, e *common.TopicEvent) (retry bool, err error) {
	log.Printf("collectEventHandler - PubsubName: %s, Topic: %s, Data: %s, \n", e.PubsubName, e.Topic, string(e.RawData))
	var event = entity.DataCollectEvent{}
	err = json.Unmarshal(e.RawData, &event)
	if err != nil {
		log.Println("collectEventHandler - data ", string(e.RawData))
		log.Println("collectEventHandler - unmarshal error: ", err)
	} else {
		go func() {
			err = service.StartDataCollect(event)
			if err != nil {
				log.Println("collectEventHandler - service error: ", err)
			}
		}()

	}

	return false, nil
}
