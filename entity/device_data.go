package entity

type CountVo struct {
	Count int `json:"count"`
}

type DeviceCurrentData struct {
	Id         string       `json:"id"`
	Identifier string       `json:"identifier"`
	Points     []PointValue `json:"points"`
	Tags       []string     `json:"tags"`
}
type PointValue struct {
	Id    string   `json:"id"`
	Name  string   `json:"name"`
	Ts    string   `json:"ts"`
	Value int      `json:"value"`
	Tags  []string `json:"tags"`
}

type DeviceHistoryData struct {
	Id         string             `json:"id"`
	Identifier string             `json:"identifier"`
	Points     []PointHistoryData `json:"points"`
}

type PointHistoryData struct {
	Id    string    `json:"id"`
	Name  string    `json:"name"`
	Datas []OneData `json:"datas"`
}
type OneData struct {
	Ts    string `json:"ts"`
	Value int    `json:"value"`
}
