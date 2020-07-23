// Package revlist provides support to access the ancestors of commits, in a
// similar way as the git-rev-list command.
package revlist

import (
	"fmt"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/storer"
)

type ConcurrentStorer interface {
	storer.EncodedObjectStorer
	Concurrency() int
}

func objectsConcurrent(
	s ConcurrentStorer,
	objects,
	ignore []plumbing.Hash,
	allowMissingObjects bool,
) ([]plumbing.Hash, error) {
	ch := make(chan plumbing.Hash)
	resCh := make(chan []plumbing.Hash)
	errCh := make(chan error)

	// Manager goroutine: Receive from ch and spawn worker goroutines.
	// Worker count is capped by ConcurrentStorer.Concurrency().
	// If manager cannot create more goroutines, it enqueues incoming hashes.
	go func() {
		semaphore := make(chan struct{}, s.Concurrency())
		// Pre-allocation to suppress allocation on each iteration
		queue := make([]plumbing.Hash, len(objects))
		for hash := range ch {
			queue := append(queue[:0], hash)
			for len(queue) > 0 {
				select {
				case hash := <-ch:
					queue = append(queue, hash)
				case semaphore <- struct{}{}:
					go func(hash plumbing.Hash) {
						defer func() { <-semaphore }()
						hs, err := findLinkedObjects(s, hash)
						if err != nil && (allowMissingObjects && err != plumbing.ErrObjectNotFound) {
							errCh <- err
							return
						}
						resCh <- hs
					}(queue[0])
					queue = queue[1:]
				}
			}
		}
	}()

	seen := hashListToSet(ignore)
	result := map[plumbing.Hash]bool{}
	itemCnt := 0
	for _, h := range objects {
		seen[h] = true
		result[h] = true
		itemCnt++
		ch <- h
	}

	var firstErr error
	for i := 0; i < itemCnt; i++ {
		select {
		case hs := <-resCh:
			// Sink results when an error has occurred.
			if firstErr != nil {
				continue
			}

			for _, h := range hs {
				if _, ok := seen[h]; !ok {
					seen[h] = true
					result[h] = true
					itemCnt++
					ch <- h
				}
			}
		case err := <-errCh:
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	close(ch)
	if firstErr != nil {
		return nil, firstErr
	}

	return hashSetToList(result), nil
}

// queueLinkedObjects writes referencing object hashes to queue channel depending of its type
func findLinkedObjects(
	s storer.EncodedObjectStorer,
	h plumbing.Hash,
) ([]plumbing.Hash, error) {
	o, err := s.EncodedObject(plumbing.AnyObject, h)
	if err != nil {
		return nil, err
	}

	do, err := object.DecodeObject(s, o)
	if err != nil {
		return nil, err
	}

	switch do := do.(type) {
	case *object.Commit:
		hs := make([]plumbing.Hash, 0, len(do.ParentHashes)+1)
		for _, hash := range do.ParentHashes {
			hs = append(hs, hash)
		}
		hs = append(hs, do.TreeHash)
		return hs, nil
	case *object.Tree:
		hs := make([]plumbing.Hash, 0, len(do.Entries))
		for _, e := range do.Entries {
			if e.Mode == filemode.Submodule {
				continue
			}
			hs = append(hs, e.Hash)
		}
		return hs, nil
	case *object.Tag:
		return []plumbing.Hash{do.Target}, nil
	case *object.Blob:
		return nil, nil
	default:
		return nil, fmt.Errorf("object type not valid: %s. "+
			"Object reference: %s", o.Type(), o.Hash())
	}
}
