package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

type Item struct {
	Key string `json:"key" csv:"key"`
	Val string `json:"val" csv:"key"`
}

func DumpToJson(db *pebble.DB) (int64, error) {
	items := make([]Item, 0)
	var count int64
	iter := db.NewIter(&pebble.IterOptions{})
	/*
		v := iter.First()
		if !v {
			fmt.Println("No item found")
			return 0, nil
		} else {
			count += 1
			fmt.Printf("Found %s with %s\n", iter.Key(), iter.Value())
			for iter.Next() {
				count += 1
			}
			fmt.Printf("=> %d items\n", count)
		}
	*/
	iter.First()
	items = append(items, Item{Key: string(iter.Key()), Val: string(iter.Value())})
	for iter.Next() {
		items = append(items, Item{Key: string(iter.Key()), Val: string(iter.Value())})
		count += 1
	}
	iterstart := iter.Stats()
	fmt.Println(iterstart)
	raw, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		fmt.Println("Failed to marshal: ", err)
	} else {
		_ = raw
		// fmt.Println(string(raw))
	}
	if err := iter.Close(); err != nil {
		fmt.Println("Failed on iter.Close(): ", err)
	}
	return count, nil
}

func main() {
	version, err := pebble.GetVersion("demo", vfs.Default)
	if err != nil {
		log.Println("Failed to retrieved version: ", err)
	} else {
		log.Println("Version is", version)
	}

	logger := pebble.DefaultLogger
	listener := pebble.MakeLoggingEventListener(logger)

	db, err := pebble.Open("demo", &pebble.Options{
		EventListener: &listener,
	})
	if err != nil {
		log.Fatal(err)
	}

	idx := 0
	for idx < 10 {
		key := []byte(fmt.Sprintf("aoto-%d", idx))
		/* pebble.Sync => we should use Async and call Sync once ? */
		if err := db.Set(key, []byte("world"), pebble.Sync); err != nil {
			log.Fatal("Failed in Set: ", string(key), err)
		}
		idx += 1
	}

	key := []byte("hello")
	err = db.Set(key, []byte("plop"), pebble.NoSync)
	if err != nil {
		log.Fatal("Failed in Set", string(key), err)
	}

	value, closer, err := db.Get(key)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s %s\n", key, value)
	if err = closer.Close(); err != nil {
		log.Fatal("Failed in closer.Close: ", err)
	}

	/* */
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("a"),
		UpperBound: []byte("b"),
	})
	v := iter.First()
	if !v {
		fmt.Println("No item found")
	} else {
		fmt.Printf("Found %s with %s\n", iter.Key(), iter.Value())
		i := 0
		for iter.Next() {
			i += 1
		}
		fmt.Printf("=> %d items\n", i)
	}
	iter.Close()

	/* XXXX */
	DumpToJson(db)

	/* trigger a flush */
	m, err := db.AsyncFlush()
	if err != nil {
		log.Println("Failed while calling AsyncFlush: ", err)
	}
	<-m
	log.Println("Flush is done")

	/* Remove previous checkpoint */
	os.RemoveAll("demo/demo-checkpoint")

	/* Create a checkpoint, checkpoint must be a child of DB path */
	ckerr := db.Checkpoint("demo/demo-checkpoint", pebble.WithFlushedWAL())
	if ckerr != nil {
		log.Fatal("Failed to create a checkpoint: ", ckerr)
	}

	db.Set([]byte("merge-test"), []byte("plop"), pebble.NoSync)
	db.Merge([]byte("merge-test"), []byte("plip"), pebble.NoSync)
	value, closer, err = db.Get([]byte("merge-test"))
	if err != nil {
		log.Fatal("Failed to Get merged key: ", err)
	}
	log.Println("===>", string(value))
	closer.Close()

	log.Println("Closing DB...")
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
}
