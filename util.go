package main

import (
	"encoding/json"
	"strconv"
	"strings"
)

func split(s string) []string {
	if s == "" {
		return []string{}
	}
	return strings.Split(s, ",")
}

func arrayEmpty(a []string) bool {
	return a == nil || len(a) == 0 || a[0] == ""
}

func toString(i interface{}) string {
	if i == nil {
		return "<null>"
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
