#!/bin/sh
mkdir -p parser
antlr4 -Dlanguage=Go -Xexact-output-dir -o parser parser/*.g4
