// Package schema extracts schema structure from CQL files.
package schema

import (
	"github.com/antlr/antlr4/runtime/Go/antlr"
	"github.com/martin-sucha/cqldoc/parser"
	"io"
	"io/ioutil"
	"reflect"
)

type Schema struct {
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

func Parse(r io.Reader) (*Schema, error) {
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
	schema := &Schema{}
	antlr.ParseTreeWalkerDefault.Walk(&documentParser{
		stream: stream,
		schema: schema,
	}, tree)

	return schema, nil
}

type documentParser struct {
	*parser.BaseCqlParserListener
	stream       *antlr.CommonTokenStream
	schema       *Schema
	currentTable *Table
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
	l.schema.Tables = append(l.schema.Tables, l.currentTable)
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