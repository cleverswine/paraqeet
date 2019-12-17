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

func getComposite(data map[string]interface{}, cols []string) string {
	result := ""
	for i := 0; i < len(cols); i++ {
		if v, ok := data[cols[i]]; ok {
			result = result + valToString(v)
		}
	}
	return result
}
