package entity

type DeviceDataInfo struct {
	DeviceIdentifier string      `json:"device_identifier"`
	Points           []PointData `json:"points"`
}

type DeviceInfoMsg struct {
	Ts         int64          `json:"ts"`
	Identifier string         `json:"identifier"`
	Properties map[string]any `json:"properties"`
}

type DeviceWithPoints struct {
	Id         string   `json:"id"`
	Identifier string   `json:"identifier"`
	ProductId  string   `json:"product_id"`
	Enabled    int      `json:"enabled"`
	Type       int      `json:"type"`
	Tags       []string `json:"tags"`
	Points     []Point  `json:"points"`
}
type Point struct {
	Id         string   `json:"id"`
	Identifier string   `json:"identifier"`
	Tags       []string `json:"tags"`
}
