package main

import (
	"encoding/json"
	"fmt"
	"github.com/martin-sucha/cqldoc/schema"
	"os"
)

func main() {
	ret, err := schema.Parse(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err.Error())
		os.Exit(1)
		return
	}
	x, err := json.MarshalIndent(ret, "", "    ")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", x)

}
