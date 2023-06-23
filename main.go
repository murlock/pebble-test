package main

import (
	"fmt"
	"log"

	"github.com/cockroachdb/pebble"
)

func main() {
	db, err := pebble.Open("demo", &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}

    idx := 0
    for idx < 100 {
        key := []byte(fmt.Sprintf("aoto-%d", idx))
        /* pebble.Sync => we should use Async and call Sync once ? */
        if err := db.Set(key, []byte("world"), pebble.Sync); err != nil {
            log.Fatal(err)
        }
        idx += 1
    }

    key := []byte("hello")
	value, closer, err := db.Get(key)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s %s\n", key, value)

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
        fmt.Printf("=> %d items\n" ,i)
    }
    iter.Close()

	if err := closer.Close(); err != nil {
		log.Fatal(err)
	}
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
}
