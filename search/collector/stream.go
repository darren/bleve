package collector

import (
	"context"
	"log"
	"runtime/debug"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
)

// StreamCollector is streaming search collector
type StreamCollector struct {
	results chan *search.DocumentMatch
	proc    func(*search.DocumentMatch) error
}

// NewStreamCollector create a streaming collector
func NewStreamCollector(proc func(*search.DocumentMatch) error) *StreamCollector {
	return &StreamCollector{
		proc: proc,
	}
}

// Collect goes to the index to find the matching documents
func (sc *StreamCollector) Collect(
	ctx context.Context,
	searcher search.Searcher,
	reader index.IndexReader,
) (chan *search.DocumentMatch, error) {

	var err error
	var results = make(chan *search.DocumentMatch, 1)

	var next *search.DocumentMatch

	searchContext := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(searcher.DocumentMatchPoolSize(), 0),
		IndexReader:       reader,
	}

	close := func() {
		reader.Close()
		searcher.Close()
		close(results)
	}

	select {
	case <-ctx.Done():
		log.Printf("Stream Done prematurely")
		return nil, ctx.Err()
	default:
		next, err = searcher.Next(searchContext)
	}

	if err != nil || next == nil {
		close()
		return results, err
	}

	if next.ID == "" {
		next.ID, err = reader.ExternalID(next.IndexInternalID)
		if err != nil {
			return nil, err
		}
	}
	next.Complete(nil)
	sc.proc(next)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				print("stacktrace from panic: \n" + string(debug.Stack()))
			}
			close()
		}()
		i := 1
		for {
			select {
			case <-ctx.Done():
				return

			case results <- next:
				next, err = searcher.Next(searchContext)
				if err != nil || next == nil {
					return
				}

				if next.ID == "" {
					next.ID, err = reader.ExternalID(next.IndexInternalID)
					if err != nil {
						return
					}
				}

				next.Complete(nil)

				err = sc.proc(next)
				if err != nil {
					return
				}
				i++
			}
		}
	}()

	return results, nil
}
