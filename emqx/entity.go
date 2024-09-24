package emqx

type Topics struct {
	Data []Topic   `json:"data"`
	Meta TopicMeta `json:"meta"`
}
type TopicMeta struct {
	Count int `json:"count"`
	Limit int `json:"limit"`
	Page  int `json:"page"`
}
type Topic struct {
	Node  string `json:"node"`
	Topic string `json:"topic"`
}
