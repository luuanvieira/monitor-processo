// Package config embute a configuração padrão de tags no binário.
package config

import _ "embed"

// TagsPadrao é o conteúdo de config/tags.json, usado quando TAGS_ARQUIVO não é informado.
//
//go:embed tags.json
var TagsPadrao []byte
