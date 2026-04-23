// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package separated

import (
	"strings"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
)

const Name = "separated"

const separators = "-_/ "

var separatorReplacer = strings.NewReplacer("_", "-", "/", "-", " ", "-")

type Tokenizer struct{}

func NewTokenizer() *Tokenizer {
	return &Tokenizer{}
}

func (t *Tokenizer) Tokenize(input []byte) analysis.TokenStream {
	token := strings.Trim(string(input), separators)
	token = separatorReplacer.Replace(token)
	return analysis.TokenStream{
		&analysis.Token{
			Term:     []byte(token),
			Position: 1,
			Start:    0,
			End:      len(token),
			Type:     analysis.AlphaNumeric,
		},
	}
}

func tokenizerConstructor(
	config map[string]interface{}, cache *registry.Cache,
) (analysis.Tokenizer, error) {
	return NewTokenizer(), nil
}

func init() {
	registry.RegisterTokenizer(Name, tokenizerConstructor)
}
