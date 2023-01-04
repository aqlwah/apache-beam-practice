// Ref: https://beam.apache.org/get-started/try-apache-beam/
package main

import (
	"context"
	"flag"
	"fmt"
	"regexp"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/direct"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"

	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"
)

var (
	input  = flag.String("input", "data/*", "Files to read.")
	output = flag.String("output", "outputs/wordcount.txt", "Output filename.")
)

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

func main() {
	flag.Parse()

	beam.Init()
	pipeline, scope := beam.NewPipelineWithRoot()

	lines := textio.Read(scope, *input)
	words := beam.ParDo(scope, func(line string, emit func(string)) {
		for _, word := range wordRE.FindAllString(line, -1) {
			emit(word)
		}
	}, lines)

	counted := stats.Count(scope, words)
	formatted := beam.ParDo(scope, func(word string, count int) string {
		return fmt.Sprintf("%s: %d", word, count)
	}, counted)
	textio.Write(scope, *output, formatted)

	direct.Execute(context.Background(), pipeline)
}
