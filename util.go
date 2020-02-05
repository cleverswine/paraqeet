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

func contains(arr []string, k string) bool {
	for i := 0; i < len(arr); i++ {
		ic := strings.ToLower(arr[i])
		s := strings.ToLower(k)
		if s == ic {
			return true
		}
		if strings.HasPrefix(ic, "*") {
			if strings.HasSuffix(s, strings.Replace(ic, "*", "", 1)) {
				return true
			}
		}
		if strings.HasSuffix(ic, "*") {
			if strings.HasPrefix(s, strings.Replace(ic, "*", "", 1)) {
				return true
			}
		}
	}
	return false
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
