package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/murlock/pebble-test/api/pb"
)

type Item struct {
	Key string `json:"key" csv:"key"`
	Val string `json:"val" csv:"key"`
}

type MyServiceServer struct {
	pb.UnimplementedServiceServer
}

func (s *MyServiceServer) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutReply, error) {
	log.Println("==>", r.Key, r.Value, r.Force)
	if r.Key == "" {
		return nil, fmt.Errorf("INVALID KEY")
	}
	return &pb.PutReply{Success: true}, nil
}

func (s *MyServiceServer) Dump(ctx context.Context, r *pb.DumpRequest) (*pb.DumpReply, error) {
	return &pb.DumpReply{Success: true}, nil
}

func DumpToJson(db *pebble.DB, file string) (int64, error) {
	items := make([]Item, 0)
	var count int64
	iter, err := db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return 0, err
	}
	defer iter.Close()

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
		if err = os.WriteFile(file, raw, 0644); err != nil {
			return 0, err
		}

	}
	return count, nil
}

func FakeServer() {
	x := pb.PutRequest{Key: "plop", Value: "zon", Force: true}
	fmt.Printf("PutRequest{Key: %s, Value: %s, Force: %t}\n", x.Key, x.Value, x.Force)
}

func DisplayMetrics(metrics *pebble.Metrics) {
	fmt.Println("==== Metrics")
	fmt.Println("== Compact")
	fmt.Println("Count", metrics.Compact.Count)
	fmt.Println("Duration", metrics.Compact.Duration)
	fmt.Println("== Ingest")
	fmt.Println("Count", metrics.Ingest.Count)
	fmt.Println("== Flush")
	fmt.Println("Count", metrics.Flush.Count)
	fmt.Println("Count", metrics.Flush.Count)
}

func MergeExample(db *pebble.DB) {
	/* example of merge: second value is added to first one */
	db.Set([]byte("merge-test"), []byte("plop"), pebble.NoSync)
	db.Merge([]byte("merge-test"), []byte("plip"), pebble.NoSync)
	value, closer, err := db.Get([]byte("merge-test"))
	if err != nil {
		log.Fatal("Failed to Get merged key: ", err)
	}
	log.Println("===>", string(value))
	closer.Close()
}

func main() {
	FakeServer()

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

	/* Example of sub scan */
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("a"),
		UpperBound: []byte("b"),
	})
	if err != nil {
		log.Fatal("Failed to create iterator: ", err)
	}
	defer iter.Close()

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

	/* XXXX */
	if _, err = DumpToJson(db, "dump.json"); err != nil {
		log.Println("Failed to dump DB to JSON")
	}

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

	metrics := db.Metrics()
	DisplayMetrics(metrics)

	log.Println("Starting GRPC server")

	/* */
	lis, err := net.Listen("tcp", "localhost:9900")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterServiceServer(grpcServer, &MyServiceServer{})
	// Register reflection service on gRPC server.
	reflection.Register(grpcServer)
	grpcServer.Serve(lis)

	/* how stop grpcServer properly ? */

	log.Println("Closing DB...")
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

}
