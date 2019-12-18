package main

import (
	"encoding/json"
	"strconv"
)

func valToString(i interface{}) string {
	if i == nil {
		return ""
	}
	switch v := i.(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.Itoa(int(v))
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}
