package util

import (
	"bytes"
	"text/template"
)

func ParseTemplate(templateStr string) *template.Template {
	return template.Must(template.New("template").Funcs(template.FuncMap{
		"EscapeIdentifier": EscapeIdentifier,
	}).Parse(templateStr))
}

func ExecTemplate(t *template.Template, data map[string]interface{}) (string, error) {
	b := bytes.NewBuffer(nil)
	if err := t.Execute(b, data); err != nil {
		return "", err
	}
	return b.String(), nil
}
