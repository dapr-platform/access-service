package entity

type DataCollectEvent struct {
	Type        string                   `json:"type"`
	Name        string                   `json:"name"`
	Devices     []DataCollectEventDevice `json:"devices"`
	DataType    string                   `json:"dataType"`
	Product     string                   `json:"product"`
	ProductName string                   `json:"productName"`
	Value       string                   `json:"value"`
}
type DataCollectEventDevice struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

type PointRWDataMetaInfo struct {
	DeviceIdentifier string         `json:"device_identifier"`
	PointIdentifier  string         `json:"point_identifier"`
	Type             string         `json:"type"`
	Properties       map[string]any `json:"properties"`
	Value            any            `json:"value"`
}
