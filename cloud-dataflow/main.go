// Ref: https://qiita.com/8kka/items/f23aa0c5b5fe9bface37
package main

import (
	"context"
	"flag"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/pubsubio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

var (
	topic = "dataflow"
	sub   = "dataflow-sub"
)

func main() {
	flag.Parse()
	ctx := context.Background()
	project := gcpopts.GetProject(ctx)

	beam.Init()
	pipeline, scope := beam.NewPipelineWithRoot()
	pc1 := pubsubio.Read(scope, project, topic, &pubsubio.ReadOptions{
		Subscription: sub,
	})
	pc2 := beam.ParDo(scope, func(b []byte) string {
		return string(b)
	}, pc1)
	debug.Printf(scope, "received value: %v", pc2)

	if err := beamx.Run(ctx, pipeline); err != nil {
		log.Exitf(ctx, "failed to pipeline execute: %v", err)
	}
}
