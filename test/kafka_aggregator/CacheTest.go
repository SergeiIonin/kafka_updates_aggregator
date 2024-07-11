package kafka_aggregator

import (
	"context"
	"fmt"
)

type FieldsCacheTest struct {
	Mapping map[string]map[string]string
}

func NewFieldsCacheTest() *FieldsCacheTest {
	return &FieldsCacheTest{
		Mapping: make(map[string]map[string]string),
	}
}

func (cache *FieldsCacheTest) Add(id string, key string, value any) error {
	_, ok := cache.Mapping[id]
	if !ok {
		cache.Mapping[id] = make(map[string]string)
	}
	cache.Mapping[id][key] = fmt.Sprintf("%v", value)
	return nil
}

func (cache *FieldsCacheTest) Get(ctx context.Context, id string, key string) (string, error) {
	return cache.Mapping[id][key], nil
}

func (cache *FieldsCacheTest) Upsert(ctx context.Context, id string, key string, value any) error {
	cache.Mapping[id][key] = fmt.Sprintf("%v", value)
	return nil
}

func (cache *FieldsCacheTest) Delete(id string, key string) error {
	delete(cache.Mapping[id], key)
	return nil
}
