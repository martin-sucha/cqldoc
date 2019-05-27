package cqldoc

import (
	"github.com/antlr/antlr4/runtime/Go/antlr"
	"io"
	"io/ioutil"
	"reflect"
	"regexp"
	"strings"

	"github.com/martin-sucha/cqldoc/parser"
)

type Document struct {
	Tables []*Table
}

type Table struct {
	Comment string
	Keyspace string
	Name string
	Columns []*Column
}

type Column struct {
	Comment string
	Name string
	CqlType string
}

type documentParser struct {
	*parser.BaseCqlParserListener
	stream *antlr.CommonTokenStream
	document *Document
	currentTable *Table
}

func (l *documentParser) EnterEveryRule(ctx antlr.ParserRuleContext) {
	//print("enter:", ctx.GetText(), "\n")
}

func (l *documentParser) ExitEveryRule(ctx antlr.ParserRuleContext) {
	//print("exit:", ctx.GetText(), "\n")
}

var regexpLineComment = regexp.MustCompile(`^(?:--|#|//)([^\r\n]*)`)

func getComment(hiddenTokens []antlr.Token) string {
	if len(hiddenTokens) == 0 {
		return ""
	}

	// Remove space at the end
	if hiddenTokens[len(hiddenTokens)-1].GetTokenType() == parser.CqlParserSPACE {
		hiddenTokens = hiddenTokens[:len(hiddenTokens)-1]

		if len(hiddenTokens) == 0 {
			return ""
		}
	}

	for _, t := range hiddenTokens {
		print(t.GetTokenType(), " ", t.GetText(), "\n")
	}

	var comment strings.Builder
	lastCommentToken := hiddenTokens[len(hiddenTokens)-1]
	switch lastCommentToken.GetTokenType() {
	case parser.CqlParserLINE_COMMENT:
		idx := len(hiddenTokens)-1
		for idx > 0 && hiddenTokens[idx-1].GetTokenType() == parser.CqlParserLINE_COMMENT {
			idx--
		}
		for _, token := range hiddenTokens[idx:] {
			m := regexpLineComment.FindStringSubmatch(token.GetText())
			comment.WriteString(m[1])
			comment.WriteString("\n")
		}
	case parser.CqlParserCOMMENT_INPUT:
		text := lastCommentToken.GetText()
		comment.WriteString(text[2:len(text)-2])
	}

	return comment.String()
}

func (l *documentParser) EnterCreateTable(ctx *parser.CreateTableContext) {

	tokens := l.stream.GetHiddenTokensToLeft(ctx.GetStart().GetTokenIndex(), antlr.TokenHiddenChannel)
	comment := getComment(tokens)

	// keyspace may be nil if not specified
	keyspace := ctx.GetChildOfType(0, reflect.TypeOf(&parser.KeyspaceContext{}))
	tableName := ctx.GetChildOfType(0, reflect.TypeOf(&parser.TableContext{}))

	var keyspaceText string
	if keyspace != nil {
		keyspaceText = keyspace.GetText()
	}

	l.currentTable = &Table{
		Comment: comment,
		Keyspace: keyspaceText,
		Name: tableName.GetText(),
	}
	l.document.Tables = append(l.document.Tables, l.currentTable)
}

func (l *documentParser) ExitCreateTable(ctx *parser.CreateTableContext) {
	l.currentTable = nil
}

func (l *documentParser) EnterColumnDefinition(ctx *parser.ColumnDefinitionContext) {
	tokens := l.stream.GetHiddenTokensToLeft(ctx.GetStart().GetTokenIndex(), antlr.TokenHiddenChannel)
	comment := getComment(tokens)

	columnName := ctx.GetChildOfType(0, reflect.TypeOf(&parser.ColumnContext{}))
	columnType := ctx.GetChildOfType(0, reflect.TypeOf(&parser.DataTypeContext{}))
	column := &Column{
		Comment: comment,
		Name: columnName.GetText(),
		CqlType: columnType.GetText(),
	}
	l.currentTable.Columns = append(l.currentTable.Columns, column)
}

func Parse(r io.Reader) (*Document, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	input := antlr.NewInputStream(string(data))
	lexer := parser.NewCqlLexer(input)
	stream := antlr.NewCommonTokenStream(lexer,0)
	p := parser.NewCqlParser(stream)
	p.AddErrorListener(antlr.NewDiagnosticErrorListener(true))
	p.BuildParseTrees = true
	tree := p.Root()
	document := &Document{}
	antlr.ParseTreeWalkerDefault.Walk(&documentParser{
		stream: stream,
		document: document,
	}, tree)

	return document, nil
}