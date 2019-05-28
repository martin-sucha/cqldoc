-- just a comment

-- table comment line 1
-- table comment line 2
CREATE TABLE sp.mytable (
    -- random comment in table

    -- column comment
    -- column comment line 2
    col1 text,

    col2 int
);

// some random comment

// slash comment line1
// slash comment line2
CREATE TABLE sp.mytable1 (
	col1 text
);

/*
 * table multiline comment
 *
 *    is here
 */
CREATE TABLE sp.mytable2 (
	col1 text
);

ALTER TABLE sp.mytable ADD col3 map<string, int>, col4 blob