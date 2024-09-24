package emqx

import (
	"encoding/json"
	"testing"
)

func TestStatus(t *testing.T) {
	client := NewAPIClient(RestAPIClientConfig{
		AppID:     "admin",
		AppSecret: "things2023",
	})
	status, err := client.Status()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(status)
}
func TestTopics(t *testing.T) {
	client := NewAPIClient(RestAPIClientConfig{
		AppID:     "admin",
		AppSecret: "things2023",
	})
	val, err := client.Topics()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(val)
	topic := Topics{}
	err = json.Unmarshal([]byte(val), &topic)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(topic)
}
