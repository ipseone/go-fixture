package fixture

import (
	"text/template"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
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
		if _, ok := funcMap[k]; !ok {
			funcMap[k] = v
		} else {
			log.Warn().Str("name", k).Msg("skipping duplicate template function")
		}
	}
}
