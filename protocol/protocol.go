package protocol

type ProtocolProcessor interface {
	GetStatus() int
	Start() error
	Stop() error
	GetProperties() map[string]any
	ReadValue(properties map[string]any) (value any, err error)
	WriteValue(properties map[string]any, value any) (err error)
}
