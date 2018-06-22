package monitor

import (
	"encoding/json"
)

type AccessLogEntry struct {
	Method       string  `json:"method"`
	Host         string  `json:"host"`
	Path         string  `json:"path"`
	IP           string  `json:"ip"`
	ResponseTime float64 `json:"response_time"`
	At           string  `json:"at"`
	At2          string  `json:"at2"`
	At3          string  `json:"at3"`
	TraceNo      string  `json:"traceno"`
	Message      string  `json:"message"`

	encoded []byte
	err     error
}

func (ale *AccessLogEntry) ensureEncoded() {
	if ale.encoded == nil && ale.err == nil {
		ale.encoded, ale.err = json.Marshal(ale)
	}
}

func (ale *AccessLogEntry) Length() int {
	ale.ensureEncoded()
	return len(ale.encoded)
}

func (ale *AccessLogEntry) Encode() ([]byte, error) {
	ale.ensureEncoded()
	return ale.encoded, ale.err
}
