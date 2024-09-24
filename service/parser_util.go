package service

import (
	"github.com/pkg/errors"
	"strconv"
)

func ValueToFloat64(v any) (r float64, err error) {
	switch v.(type) {
	case float64:
		r = v.(float64)
	case string:
		r, err = strconv.ParseFloat(v.(string), 10)
		return
	case float32:
		r = float64(v.(float32))
	case int:
		r = float64(v.(int))
	default:
		err = errors.New("can't deal with v. type ")
	}
	return
}
