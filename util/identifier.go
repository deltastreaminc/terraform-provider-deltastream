package util

import "strings"

func ParseIdentifier(id string) string {
	id = strings.TrimSpace(id)
	if strings.HasPrefix(id, `"`) {
		return strings.ReplaceAll(id[1:len(id)-1], `""`, `"`)
	}
	if strings.HasPrefix(id, "`") {
		return strings.ReplaceAll(id[1:len(id)-1], "``", "`")
	}
	return strings.ToLower(id)
}

func EscapeIdentifier(i string) string {
	if i == "" {
		return ``
	}
	return strings.ReplaceAll(i, `"`, `""`)
}
