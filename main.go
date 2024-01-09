package main

import (
	"fmt"

	"github.com/bzick/tokenizer"
)

// Define token types as regular expressions
var tokenDefs = []struct {
	name    string
	pattern string
}{
	{"PIPE", `\|`},   // Match pipe symbol (|)
	{"LPAREN", `\(`}, // Match left parentheses
	{"RPAREN", `\)`}, // Match right parentheses
	{"COMMA", `,`},   // Match comma
	{"BOOLEAN_EXPRESSION", `(if|else|elseif|true|false|and|or|not|[=<>]=?)`}, // Match boolean expressions
	{"STRING", `'[^']*'`},                                         // Match single-quoted strings
	{"GROUPBY", `groupby`},                                        // Match 'groupby' keyword
	{"COL_LIST", `\[[^\]]*\]`},                                    // Match a list of columns inside square brackets
	{"TIME_WINDOW", `window\('\d+[smhDWMy]'\)`},                   // Match time window specification (e.g., window('1m'))
	{"RATE_FUNCTION", `rate\('\d+[smhDWMy]',\s*'\d+[smhDWMy]'\)`}, // Match rate function (e.g., rate('20m', '1m'))
	{"SORT_FUNCTION", `sort\(\[[^\]]*\]\)`},                       // Match sort function with a list of columns (e.g., sort(['col1', 'col2']))
	{"LIMIT_FUNCTION", `limit\(\d+\)`},                            // Match limit function (e.g., limit(10))
	{"IDENTIFIER", `[a-zA-Z_][a-zA-Z0-9_]*`},                      // Match identifiers (e.g., metric_name)
	{"FUNCTION", `[a-zA-Z_]\w*`},                                  // Match function names (e.g., sum, filter, groupby, window, rate, sort, limit)
	{"NUMBER", `\d+(\.\d+)?`},                                     // Match numbers (e.g., 123, 3.14)
}

/*

// Token represents a token with a name and value
type Token struct {
	Name  string
	Value string
}

// Lexer tokenizes the input query
func lexer(query string) ([]Token, error) {
	tokens := []Token{}
	tokenPattern := ""
	for _, td := range tokenDefs {
		tokenPattern += fmt.Sprintf(`(?P<%s>%s)|`, td.name, td.pattern)
	}
	tokenPattern = tokenPattern[:len(tokenPattern)-1] // Remove the trailing "|"

	regex := regexp.MustCompile(tokenPattern)
	matches := regex.FindAllStringSubmatch(query, -1)

	if matches == nil {
		return nil, fmt.Errorf("Failed to tokenize the input query")
	}

	for _, match := range matches {
		for i, name := range regex.SubexpNames() {
			if name != "" {
				tokenName := name
				tokenValue := match[i]
				tokens = append(tokens, Token{tokenName, tokenValue})
			}
		}
	}

	return tokens, nil
}
*/

// define custom tokens keys
const (
	TPipe              = 1
	TLParen            = 2
	TRParen            = 3
	TListOpen          = 4
	TListClose         = 5
	TComma             = 6
	TBooleanExpression = 7
	TFunctions         = 8
	TList              = 9
	TBinaryOperators   = 10
	TDot               = 11
	TMath              = 12
	TString            = 13
)

func main() {
	query := "filter(if time > \"2022-01-01\" and temperature > 25 then true else false) | groupby([\"region\", \"department\"]) | window(1h) | rate(20m, 1m) | sort([\"column1\", \"column2\"]) | limit(10) | sum(cpu_usage)"

	// configure tokenizer
	parser := tokenizer.New()

	parser.DefineTokens(TPipe, []string{"|"})
	parser.DefineTokens(TLParen, []string{"("})
	parser.DefineTokens(TRParen, []string{")"})
	parser.DefineTokens(TListOpen, []string{"["})
	parser.DefineTokens(TListClose, []string{"]"})
	parser.DefineTokens(TComma, []string{","})
	parser.DefineTokens(TBooleanExpression, []string{"if", "else", "elseif", "then", "true", "false", "and", "not"})
	parser.DefineTokens(TFunctions, []string{"filter", "groupby", "rate", "sort", "window", "limit"})
	parser.DefineTokens(TBinaryOperators, []string{"<", "<=", "==", ">=", ">", "!=", "?"})
	parser.DefineTokens(TDot, []string{"."})
	parser.DefineTokens(TMath, []string{"+", "-", "/", "*", "%"})
	parser.DefineStringToken(TString, `"`, `"`).SetEscapeSymbol(tokenizer.BackSlash)

	// create tokens stream
	//stream := parser.ParseString(`user_id = 119 and modified > "2020-01-01 00:00:00" or amount >= 122.34`)
	stream := parser.ParseString(query)
	defer stream.Close()

	// iterate over each token
	for stream.IsValid() {
		//if stream.CurrentToken().Is(tokenizer.TokenKeyword) {
		token := stream.CurrentToken()
		//field := stream.NextToken().ValueString()
		fmt.Println(token)
		stream.GoNext()
	}
}
