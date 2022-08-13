package firestore

import (
	"context"

	"cloud.google.com/go/firestore"
	"github.com/go-joe/joe"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Config struct {
	Project    string
	Collection string
	Logger     *zap.Logger
}

type memory struct {
	logger     *zap.Logger
	Client     *firestore.Client
	collection *firestore.CollectionRef
	ctx        context.Context
}

type DocumentStore struct {
	Value []byte `firestore:"value"`
}

func Memory(project string, opts ...Option) joe.Module {
	return joe.ModuleFunc(func(joeConf *joe.Config) error {
		conf := Config{Project: project}
		for _, opt := range opts {
			err := opt(&conf)
			if err != nil {
				return err
			}
		}

		if conf.Logger == nil {
			conf.Logger = joeConf.Logger("firestore")
		}

		memory, err := NewMemory(conf)
		if err != nil {
			return err
		}
		joeConf.SetMemory(memory)
		return nil
	})
}

func NewMemory(conf Config) (joe.Memory, error) {
	if conf.Logger == nil {
		conf.Logger = zap.NewNop()
	}
	if conf.Collection == "" {
		conf.Collection = "joe-bot"
	}

	memory := &memory{
		logger: conf.Logger,
	}
	memory.logger.Debug("Connecting to FireStore",
		zap.String("project", conf.Project),
	)

	ctx := context.Background()
	fsClient, err := firestore.NewClient(ctx, conf.Project)
	if err != nil {
		return nil, err
	}
	memory.ctx = ctx
	memory.Client = fsClient
	memory.collection = fsClient.Collection(conf.Collection)

	memory.logger.Info("Firestore initialized successfully")
	return memory, nil
}

func (b *memory) Set(document string, value []byte) error {
	doc := b.collection.Doc(document)
	b.logger.Debug("Storing data " + document)
	_, err := doc.Set(b.ctx, DocumentStore{
		Value: value,
	})
	return err
}

func (b *memory) Get(document string) ([]byte, bool, error) {
	doc := b.collection.Doc(document)
	resp, err := doc.Get(b.ctx)
	switch {
	case status.Code(err) == codes.NotFound:
		return nil, false, nil
	case err != nil:
		return nil, false, err
	default:
		var d DocumentStore
		resp.DataTo(&d)
		return d.Value, true, nil
	}
}

func (b *memory) Delete(document string) (bool, error) {
	doc := b.collection.Doc(document)
	_, err := doc.Delete(b.ctx)
	return err != nil, err
}

func (b *memory) Keys() ([]string, error) {
	keys := []string{}
	docs := b.collection.Documents(b.ctx)
	for {
		doc, err := docs.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		keys = append(keys, doc.Ref.ID)
	}
	return keys, nil
}

func (b *memory) Close() error {
	return b.Client.Close()
}
