package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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
	db *pebble.DB
}

func (s *MyServiceServer) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutReply, error) {
	slog.Info("Put", "key", r.Key, "value", r.Value, "force", r.Force)
	if r.Key == "" {
		return nil, fmt.Errorf("INVALID KEY")
	}
	success := true
	err := s.db.Set([]byte(r.Key), []byte(r.Value), pebble.NoSync)
	if err != nil {
		slog.Error("Failed in Set", "key", r.Key, "error", err)
		success = false
	}
	return &pb.PutReply{Success: success}, nil
}

func (s *MyServiceServer) Dump(ctx context.Context, r *pb.DumpRequest) (*pb.DumpReply, error) {
	// Apply default if field not set
	output := r.GetOutput()
	if r.Output == nil {
		output = "default_dump.json" // Apply custom default
	}
	if output == "" {
		return nil, fmt.Errorf("output field cannot be empty")
	}

	success := true
	_, err := DumpToJson(s.db, output)
	if err != nil {
		success = false
	}
	return &pb.DumpReply{Success: success}, nil
}

func DumpToJson(db *pebble.DB, file string) (int64, error) {
	items := make([]Item, 0)
	var count int64
	iter, err := db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return 0, err
	}
	defer func() {
		err := iter.Close()
		if err != nil {
			slog.Warn("An error occurred on iter.Close()", "error", err)
		}
	}()

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
	/* what it this code ? */
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
	err := db.Set([]byte("merge-test"), []byte("plop"), pebble.NoSync)
	if err != nil {
		slog.Warn("An error occured on db.Set", "error", err)
	}
	err = db.Merge([]byte("merge-test"), []byte("plip"), pebble.NoSync)
	if err != nil {
		slog.Warn("An error occured on db.Merge", "error", err)
	}
	value, closer, err := db.Get([]byte("merge-test"))
	if err != nil {
		slog.Error("Failed to Get merged key", "error", err)
		os.Exit(1)
	}
	slog.Info("MergeExample", "value", string(value))
	err = closer.Close()
	if err != nil {
		slog.Warn("An error occured on closer.Close", "error", err)
	}
}

func main() {
	FakeServer()

	version, err := pebble.GetVersion("demo", vfs.Default)
	if err != nil {
		slog.Error("demo: failed to retrieve version", "error", err)
	} else {
		slog.Info("demo info", "version", version)
	}

	logger := pebble.DefaultLogger
	listener := pebble.MakeLoggingEventListener(logger)

	db, err := pebble.Open("demo", &pebble.Options{
		EventListener: &listener,
	})
	if err != nil {
		slog.Error("failed to open demo db", "error", err)
		os.Exit(1)
	}

	idx := 0
	for idx < 10 {
		key := []byte(fmt.Sprintf("aoto-%d", idx))
		/* pebble.Sync => we should use Async and call Sync once ? */
		if err := db.Set(key, []byte("world"), pebble.Sync); err != nil {
			slog.Error("Failed in Set", "key", string(key), "error", err)
			os.Exit(1)
		}
		idx += 1
	}

	key := []byte("hello")
	err = db.Set(key, []byte("plop"), pebble.NoSync)
	if err != nil {
		slog.Error("Failed in Set", "key", string(key), "error", err)
		os.Exit(1)
	}

	value, closer, err := db.Get(key)
	if err != nil {
		slog.Error("Failed to get key", "key", string(key), "error", err)
		os.Exit(1)
	}
	fmt.Printf("%s %s\n", key, value)
	if err = closer.Close(); err != nil {
		slog.Error("Failed in closer.Close", "error", err)
		os.Exit(1)
	}

	/* Example of sub scan */
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("a"),
		UpperBound: []byte("b"),
	})
	if err != nil {
		slog.Error("failed to create iterator", "error", err)
		os.Exit(1)
	}
	defer func() {
		err := iter.Close()
		if err != nil {
			slog.Warn("An error occurred on iter.Close()", "error", err)
		}
	}()

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
		slog.Error("Failed to dump DB to JSON", "error", err)
	}

	/* trigger a flush */
	m, err := db.AsyncFlush()
	if err != nil {
		slog.Error("Failed while calling AsyncFlush", "error", err)
	}
	<-m
	slog.Info("Flush is done")

	/* Remove previous checkpoint */
	err = os.RemoveAll("demo/demo-checkpoint")
	if err != nil {
		slog.Error("failed to remove demo-checkpoint", "error", err)
	}

	/* Create a checkpoint, checkpoint must be a child of DB path */
	ckerr := db.Checkpoint("demo/demo-checkpoint", pebble.WithFlushedWAL())
	if ckerr != nil {
		slog.Error("Failed to create a checkpoint", "error", ckerr)
		os.Exit(1)
	}

	metrics := db.Metrics()
	DisplayMetrics(metrics)

	slog.Info("Starting GRPC server")

	/* */
	tcpListener, err := net.Listen("tcp", "localhost:9900")
	if err != nil {
		slog.Error("Failed to listening on localshot:9900", "error", err)
		os.Exit(1)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterServiceServer(grpcServer, &MyServiceServer{db: db})
	// Register reflection service on gRPC server.
	reflection.Register(grpcServer)
	err = grpcServer.Serve(tcpListener)
	if err != nil {
		slog.Error("failed to start grpcServer", "error", err)
	}

	/* how stop grpcServer properly ? */

	slog.Info("Closing DB...")
	if err := db.Close(); err != nil {
		slog.Error("An error occured while closing DB", "error", err)
		os.Exit(1)
	}

}
