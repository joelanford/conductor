package operators

import (
	"context"

	"github.com/ChimeraCoder/anaconda"
	"github.com/joelanford/conductor"
)

type TwitterStream struct {
	api               *anaconda.TwitterApi
	consumerKey       string
	consumerSecret    string
	accessToken       string
	accessTokenSecret string

	oc conductor.OperatorContext
}

func NewTwitterStream(consumerKey, consumerSecret, accessToken, accessTokenSecret string) conductor.CreateSpoutProcessorFunc {
	return func() conductor.SpoutProcessor {
		return &TwitterStream{
			consumerKey:       consumerKey,
			consumerSecret:    consumerSecret,
			accessToken:       accessToken,
			accessTokenSecret: accessTokenSecret,
		}
	}
}

func (s *TwitterStream) Setup(ctx context.Context, oc conductor.OperatorContext) {
	s.oc = oc

	anaconda.SetConsumerKey(s.consumerKey)
	anaconda.SetConsumerSecret(s.consumerSecret)
	s.api = anaconda.NewTwitterApi(s.accessToken, s.accessTokenSecret)
}

func (s *TwitterStream) Process(ctx context.Context) {
	stream := s.api.PublicStreamSample(nil)
	defer stream.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-stream.C:
			if tweet, ok := msg.(anaconda.Tweet); ok {
				s.oc.Submit(conductor.TupleData{"tweet": tweet}, 0)
			}
		}
	}
}

func (s *TwitterStream) Teardown() {}
