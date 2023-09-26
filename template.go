package fixture

import (
	"text/template"

	"github.com/google/uuid"
)

var funcMap template.FuncMap

func defaultFuncMap() template.FuncMap {
	fixtureID := uuid.New().String()

	return template.FuncMap{
		"fixtureID": func() string {
			return fixtureID
		},
	}
}

func AddFuncMap(fm template.FuncMap) {
	if funcMap == nil {
		funcMap = defaultFuncMap()
	}

	for k, v := range fm {
		funcMap[k] = v
	}
}
