package elastic

import (
	"fmt"
	"testing"

	"github.com/RedisLabs/RediSearchBenchmark/index"
	"github.com/RedisLabs/RediSearchBenchmark/query"
	"github.com/stretchr/testify/assert"
)

func TestIndex(t *testing.T) {
	// todo: run redisearch automatically
	md := index.NewMetadata().AddField(index.NewTextField("title", 1.0)).
		AddField(index.NewNumericField("score"))

	idx := NewIndex("http://localhost:9200", "testung", md)

	docs := []index.Document{}
	for i := 0; i < 100; i++ {
		docs = append(docs, index.NewDocument(fmt.Sprintf("doc%d", i), 0.1).Set("title", "hello world").Set("body", "lorem ipsum foo bar"))

		//index.NewDocument("doc2", 1.0).Set("title", "foo bar hello").Set("score", 2),
	}

	assert.NoError(t, idx.Drop())
	assert.NoError(t, idx.Create())

	assert.NoError(t, idx.Index(docs, nil))
	return
	q := query.NewQuery("doc", "hello world")
	docs, total, err := idx.Search(*q)
	assert.NoError(t, err)
	assert.True(t, total > 0)
	assert.Len(t, docs, 1)
	assert.Equal(t, docs[0].Id, "doc1")
	assert.Equal(t, docs[0].Properties["title"], "hello world")

	q = query.NewQuery("doc", "hello")
	docs, total, err = idx.Search(*q)
	assert.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Len(t, docs, 2)
	assert.Equal(t, docs[0].Id, "doc2")
	assert.Equal(t, docs[1].Id, "doc1")

}
