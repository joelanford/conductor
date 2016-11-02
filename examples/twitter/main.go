package main

import (
	"context"
	"log"
	"time"

	"flag"

	"github.com/ChimeraCoder/anaconda"
	"github.com/joelanford/conductor"
	"github.com/joelanford/conductor/operators"
)

func main() {
	consumerKey := flag.String("consumerKey", "", "Twitter API consumer key")
	consumerSecret := flag.String("consumerSecret", "", "Twitter API consumer secret")
	accessToken := flag.String("accessToken", "", "Twitter API access token")
	accessTokenSecret := flag.String("accessTokenSecret", "", "Twitter API access token secret")
	flag.Parse()

	if *consumerKey == "" {
		log.Fatal("-consumerKey is required")
	}
	if *consumerSecret == "" {
		log.Fatal("-consumerSecret is required")
	}
	if *accessToken == "" {
		log.Fatal("-accessToken is required")
	}
	if *accessTokenSecret == "" {
		log.Fatal("-accessTokenSecret is required")
	}

	hashtagMapper := func(t conductor.Tuple) []conductor.TupleData {
		if tweet, ok := t.Data["tweet"].(anaconda.Tweet); ok {
			hashtags := make([]conductor.TupleData, len(tweet.Entities.Hashtags))
			for i, hashtag := range tweet.Entities.Hashtags {
				hashtags[i] = conductor.TupleData{"hashtag": hashtag.Text}
			}
			return hashtags
		}
		return nil
	}

	t := conductor.NewTopology("my-topology")

	t.AddSpout("twitterstream", operators.NewTwitterStream(*consumerKey, *consumerSecret, *accessToken, *accessTokenSecret), 1).
		Produces("twitter")
	t.AddBolt("hashtagmapper", operators.NewMap(hashtagMapper), 1).
		Consumes("twitter", conductor.PartitionRoundRobin(), 1000).
		Produces("hashtags")
	t.AddBolt("topk", operators.NewTopN(100000000, 5, 10, "hashtag"), 1).
		Consumes("hashtags", conductor.PartitionRoundRobin(), 1000).
		Produces("tophashtags")
	t.AddBolt("printer", operators.NewTupleLogger(), 2).
		Consumes("tophashtags", conductor.PartitionRoundRobin(), 1000)

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour*24*265)
	t.Run(ctx)
	cancel()
}
