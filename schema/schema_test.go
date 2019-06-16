package schema

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"github.com/stretchr/testify/require"
)

func TestCreateTableSingle(t *testing.T) {
	schema, err := ParseString(`-- random comment

-- table comment line 1
-- table comment line 2
CREATE TABLE sp.mytable (
    -- random comment in table

    -- column comment
    -- column comment line 2
    col1 text,

    col2 int
);`)
	require.NoError(t, err)
	require.NotNil(t, schema)
	require.Equal(t, 1, len(schema.Tables))
	table := schema.GetTable("sp", "mytable")
	require.NotNil(t, table)
	assert.Equal(t, "table comment line 1\ntable comment line 2", table.Comment)
	require.Equal(t, 2, len(table.Columns))

	col1 := table.GetColumn("col1")
	require.NotNil(t, col1)
	assert.Equal(t, "column comment\ncolumn comment line 2", col1.Comment)
	assert.Equal(t, "text", col1.CqlType)
	assert.Equal(t, "col1", col1.Name)

	col2 := table.GetColumn("col2")
	require.NotNil(t, col2)
	assert.Equal(t, "", col2.Comment)
	assert.Equal(t, "int", col2.CqlType)
	assert.Equal(t, "col2", col2.Name)
}

func TestAlterTableAdd(t *testing.T) {
	schema, err := ParseString(`CREATE TABLE ab.tbl (col1 text, col2 text);
ALTER TABLE ab.tbl ADD
	-- col3 comment
	col3 map<string, int>, /* col4 comment */col4 blob
	;
`)
	require.NoError(t, err)
	require.NotNil(t, schema)
	require.Equal(t, 1, len(schema.Tables))
	table := schema.GetTable("ab", "tbl")
	require.NotNil(t, table)
	require.Equal(t, 4, len(table.Columns))

	col1 := table.GetColumn("col1")
	require.NotNil(t, col1)
	assert.Equal(t, "", col1.Comment)
	assert.Equal(t, "text", col1.CqlType)
	assert.Equal(t, "col1", col1.Name)

	col2 := table.GetColumn("col2")
	require.NotNil(t, col2)
	assert.Equal(t, "", col2.Comment)
	assert.Equal(t, "text", col2.CqlType)
	assert.Equal(t, "col2", col2.Name)

	col3 := table.GetColumn("col3")
	require.NotNil(t, col3)
	assert.Equal(t, "col3 comment", col3.Comment)
	assert.Equal(t, "map<string,int>", col3.CqlType)
	assert.Equal(t, "col3", col3.Name)

	col4 := table.GetColumn("col4")
	require.NotNil(t, col4)
	assert.Equal(t, "col4 comment ", col4.Comment)
	assert.Equal(t, "blob", col4.CqlType)
	assert.Equal(t, "col4", col4.Name)
}

func TestAlterTableRename(t *testing.T) {
	schema, err := ParseString(`CREATE TABLE ab.tbl (col1 text, col2 text);
ALTER TABLE ab.tbl RENAME col2 TO col5;
`)
	require.NoError(t, err)
	require.NotNil(t, schema)
	require.Equal(t, 1, len(schema.Tables))
	table := schema.GetTable("ab", "tbl")
	require.NotNil(t, table)
	require.Equal(t, 2, len(table.Columns))

	col1 := table.GetColumn("col1")
	require.NotNil(t, col1)
	assert.Equal(t, "", col1.Comment)
	assert.Equal(t, "text", col1.CqlType)
	assert.Equal(t, "col1", col1.Name)

	col5 := table.GetColumn("col5")
	require.NotNil(t, col5)
	assert.Equal(t, "", col5.Comment)
	assert.Equal(t, "text", col5.CqlType)
	assert.Equal(t, "col5", col5.Name)
}

func TestAlterTableRenameNonExistingTable(t *testing.T) {
	schema, err := ParseString(`CREATE TABLE ab.tbl (col1 text, col2 text);
ALTER TABLE ab.tbl2 RENAME col1 TO col5;
`)
	require.Error(t, err)
	require.Nil(t, schema)
}

func TestAlterTableRenameNonExistingColumn(t *testing.T) {
	schema, err := ParseString(`CREATE TABLE ab.tbl (col1 text, col2 text);
ALTER TABLE ab.tbl RENAME col3 TO col5;
`)
	require.Error(t, err)
	require.Nil(t, schema)
}

func TestAlterTableRenameDuplicateColumn(t *testing.T) {
	schema, err := ParseString(`CREATE TABLE ab.tbl (col1 text, col2 text);
ALTER TABLE ab.tbl RENAME col1 TO col2;
`)
	require.Error(t, err)
	require.Nil(t, schema)
}

func TestAlterTableDrop(t *testing.T) {
	schema, err := ParseString(`CREATE TABLE ab.tbl (col1 text, col2 text);
ALTER TABLE ab.tbl DROP col1;
`)
	require.NoError(t, err)
	require.NotNil(t, schema)
	require.Equal(t, 1, len(schema.Tables))
	table := schema.GetTable("ab", "tbl")
	require.NotNil(t, table)
	require.Equal(t, 1, len(table.Columns))

	col2 := table.GetColumn("col2")
	require.NotNil(t, col2)
	assert.Equal(t, "", col2.Comment)
	assert.Equal(t, "text", col2.CqlType)
	assert.Equal(t, "col2", col2.Name)
}

func TestAlterTableDropNonExistingTable(t *testing.T) {
	schema, err := ParseString(`CREATE TABLE ab.tbl (col1 text, col2 text);
ALTER TABLE ab2.tbl DROP col1;
`)
	require.Error(t, err)
	require.Nil(t, schema)
}

func TestAlterTableDropNonExistingColumn(t *testing.T) {
	schema, err := ParseString(`CREATE TABLE ab.tbl (col1 text, col2 text);
ALTER TABLE ab.tbl DROP col3;
`)
	require.Error(t, err)
	require.Nil(t, schema)
}