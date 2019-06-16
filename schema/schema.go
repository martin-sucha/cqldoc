// Package schema extracts schema structure from CQL files.
package schema

import (
	"bytes"
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

type ParseError struct {
	Message string
}

func (pe *ParseError) Error() string {
	return pe.Message
}

// GetTable finds a table with the keyspace and name.
// Returns nil if not found.
func (s *Schema) GetTable(keyspace, name string) *Table {
	for _, t := range s.Tables {
		if t.Keyspace == keyspace && t.Name == name {
			return t
		}
	}
	return nil
}

// GetColumn finds a column by name.
// Returns nil if not found.
func (s *Table) GetColumn(name string) *Column {
	for _, column := range s.Columns {
		if column.Name == name {
			return column
		}
	}
	return nil
}

// DropColumn drops a column.
// Does nothing if not found.
func (s *Table) DropColumn(name string) {
	for idx, column := range s.Columns {
		if column.Name == name {
			copy(s.Columns[idx:], s.Columns[idx+1:])
			s.Columns[len(s.Columns)-1] = nil
			s.Columns = s.Columns[:len(s.Columns)-1]
			return
		}
	}
}

// RenameColumn renames a column.
func (s *Table) RenameColumn(oldName, newName string) {
	oldColumn := s.GetColumn(oldName)
	if oldColumn == nil {
		panic(&ParseError{"Column does not exist"})
	}
	newColumn := s.GetColumn(newName)
	if newColumn != nil {
		panic(&ParseError{"Duplicate column found"})
	}
	oldColumn.Name = newName
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

	err = recoverParseError(func() {
		antlr.ParseTreeWalkerDefault.Walk(&documentParser{
			stream: stream,
			schema: schema,
		}, tree)
	})

	if err != nil {
		return nil, err
	}

	return schema, nil
}

func recoverParseError(f func()) (err error) {
	defer func() {
		recovered := recover()
		if recovered == nil {
			return
		}
		if recoveredErr, ok := recovered.(*ParseError); ok {
			err = recoveredErr
		} else {
			panic(recovered)
		}
	}()
	f()
	return nil
}

func ParseString(cql string) (*Schema, error) {
	return Parse(bytes.NewReader([]byte(cql)))
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

func (l *documentParser) EnterAlterTable(ctx *parser.AlterTableContext) {
	// keyspace may be nil if not specified
	keyspace := ctx.GetChildOfType(0, reflect.TypeOf(&parser.KeyspaceContext{}))
	tableName := ctx.GetChildOfType(0, reflect.TypeOf(&parser.TableContext{}))

	var keyspaceText string
	if keyspace != nil {
		keyspaceText = keyspace.GetText()
	}

	l.currentTable = l.schema.GetTable(keyspaceText, tableName.GetText())
	if l.currentTable == nil {
		panic(&ParseError{Message: "Table not found"})
	}
}

func (l *documentParser) ExitAlterTable(ctx *parser.AlterTableContext) {
	l.currentTable = nil
}

func (l *documentParser) EnterAlterTableColumnDefinition(ctx *parser.AlterTableColumnDefinitionContext) {
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

func (l *documentParser) EnterAlterTableDropColumnList(ctx *parser.AlterTableDropColumnListContext) {
	for _, child := range ctx.GetChildren() {
		if column, ok := child.(*parser.ColumnContext); ok {
			l.currentTable.DropColumn(column.GetText())
		}
	}
}

func (l *documentParser) EnterAlterTableRename(ctx *parser.AlterTableRenameContext) {
	oldName := ctx.GetChildOfType(0, reflect.TypeOf(&parser.ColumnContext{})).GetText()
	newName := ctx.GetChildOfType(1, reflect.TypeOf(&parser.ColumnContext{})).GetText()
	l.currentTable.RenameColumn(oldName, newName)
}

