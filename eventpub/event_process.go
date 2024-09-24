package eventpub

import (
	"context"
	"github.com/dapr-platform/common"
)

func PublishEvent(ctx context.Context, pubsub, topic string, event any) error {

	return common.GetDaprClient().PublishEvent(ctx, pubsub, topic, event)
}
