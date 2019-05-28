package schema

import (
	"bufio"
	"bytes"
	"github.com/antlr/antlr4/runtime/Go/antlr"
	"github.com/martin-sucha/cqldoc/parser"
	"regexp"
	"strings"
)

var regexpLineComment = regexp.MustCompile(`^(?:--|#|//)([^\r\n]*)`)

// trimStarLine removes [ \t]+*? in the first maxColumn characters
func trimStarLine(line string, maxColumn int) string {
	if len(line) < maxColumn {
		return line
	}
	column := 0
	for idx, r := range line {
		if column == maxColumn {
			return line[idx:]
		}
		switch r {
		case '\t':
		case ' ':
			column += 1
		case '*':
			if column != maxColumn-1 {
				return line
			}
			column += 1
		default:
			return line
		}
	}
	return ""
}

func getComment(hiddenTokens []antlr.Token) string {
	if len(hiddenTokens) == 0 {
		return ""
	}

	// Remove space at the end
	if hiddenTokens[len(hiddenTokens)-1].GetTokenType() == parser.CqlParserHORIZONTAL_SPACE {
		hiddenTokens = hiddenTokens[:len(hiddenTokens)-1]

		if len(hiddenTokens) == 0 {
			return ""
		}
	}

	if len(hiddenTokens) >= 2 && hiddenTokens[len(hiddenTokens)-1].GetTokenType() == parser.CqlParserVERTICAL_SPACE &&
		hiddenTokens[len(hiddenTokens)-2].GetTokenType() == parser.CqlParserCOMMENT_INPUT {
		hiddenTokens = hiddenTokens[:len(hiddenTokens)-1]
	}

	var comment []string
	lastCommentToken := hiddenTokens[len(hiddenTokens)-1]
	switch lastCommentToken.GetTokenType() {
	case parser.CqlParserLINE_COMMENT:
		idx := len(hiddenTokens)-1
	GatherTokens:
		for ; idx > 0; idx-- {
			switch hiddenTokens[idx-1].GetTokenType() {
			case parser.CqlParserLINE_COMMENT:
			case parser.CqlParserHORIZONTAL_SPACE:
			default:
				break GatherTokens
			}
		}
		for idx < len(hiddenTokens) && hiddenTokens[idx].GetTokenType() == parser.CqlParserHORIZONTAL_SPACE {
			idx++
		}
		for _, token := range hiddenTokens[idx:] {
			if token.GetTokenType() != parser.CqlParserLINE_COMMENT {
				continue
			}

			m := regexpLineComment.FindStringSubmatch(token.GetText())
			comment = append(comment, m[1])
		}
	case parser.CqlParserCOMMENT_INPUT:
		text := lastCommentToken.GetText()
		column := lastCommentToken.GetColumn()
		scanner := bufio.NewScanner(bytes.NewReader([]byte(text)[2:len(text)-2]))
		for scanner.Scan() {
			line := scanner.Text()
			line = trimStarLine(line, column+2)
			comment = append(comment, line)
		}
	}

	return strings.Join(unindentBlock(comment), "\n")
}

func unindentBlock(lines []string) []string {
	firstNonEmptyLineIndex := 0
	for firstNonEmptyLineIndex < len(lines) {
		if len(lines[firstNonEmptyLineIndex]) > 0 {
			break
		}
		firstNonEmptyLineIndex++
	}
	nonEmptyLines := lines[firstNonEmptyLineIndex:]
	if len(nonEmptyLines) == 0 {
		return lines
	}
	min := countLeft(nonEmptyLines[0], ' ')
	for _, line := range nonEmptyLines[1:] {
		if len(line) == 0 {
			continue
		}
		l := countLeft(line, ' ')
		if l < min {
			min = l
		}
	}
	ret := make([]string, len(lines))
	for idx, line := range lines {
		ret[idx] = trimLeft(line, min)
	}
	return ret
}

// countLeft counts how many r are in the beginning of s
func countLeft(s string, r rune) int {
	idx := 0
	for _, c := range s {
		if c != r {
			return idx
		}
		idx += 1
	}
	return idx
}

// trimLeft removes first runeCount runes from the beginning of s
func trimLeft(s string, runeCount int) string {
	runeIndex := 0
	for idx := range s {
		if runeIndex == runeCount {
			return s[idx:]
		}
		runeIndex += 1
	}
	return ""
}