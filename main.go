package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	bdg "github.com/dgraph-io/badger/v2"
	"github.com/dustin/go-humanize"
	badger "github.com/ipfs/go-ds-badger2"
	pebbleds "github.com/ipfs/go-ds-pebble"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-car"
	"github.com/urfave/cli/v2"

	"github.com/raulk/lotus-bs-bench/bbolt"
	fasterbs "github.com/raulk/lotus-bs-bench/faster"
	lmdbbs "github.com/raulk/lotus-bs-bench/lmdb"
	sqlite3bs "github.com/raulk/lotus-bs-bench/sqlite3"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	app := &cli.App{
		Name:  "bs-bench",
		Usage: "Benchmark performance of IPFS blockstores",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "store-type",
				Usage:    "store type to use: 'badger', 'sqlite3', 'pebble', 'lmdb', 'boltdb', 'faster",
				Required: true,
			},
			&cli.StringFlag{
				Name:      "store-path",
				Usage:     "path to the store on disk; may have to be a directory or a File, depending on the store",
				Required:  true,
				TakesFile: true,
			},
			&cli.BoolFlag{
				Name:  "import",
				Usage: "import into the blockstore",
				Value: true,
			},
			&cli.BoolFlag{
				Name:  "read",
				Usage: "read from the blockstore",
				Value: true,
			},
			&cli.StringFlag{
				Name:      "car",
				Usage:     "CAR file to import or whose manifest to read from",
				Required:  true,
				TakesFile: true,
			},
			&cli.StringFlag{
				Name:  "import-limit",
				Usage: "maximum number of CIDs to import from the CAR; if absent, we'll import all; this is a fuzzy limit (esp. when batch puts are used)",
			},
			&cli.StringFlag{
				Name:  "read-count",
				Usage: "number of reads to perform; if absent, we'll read all CIDs",
			},
			&cli.Int64Flag{
				Name:  "read-repeat-window",
				Usage: "number of recently read CIDs that form the repeat window",
				Value: 0,
			},
			&cli.Float64Flag{
				Name:  "read-repeat-rate",
				Usage: "proportion of reads to be repeated from the repeat window",
				Value: 0,
			},
		},
		Action: run,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func run(c *cli.Context) (err error) {
	// validate params.
	if rr := c.Float64("read-repeate-rate"); rr < 0 || rr > 1 {
		return fmt.Errorf("read repeat rate must be a float between 0 and 1")
	}

	var (
		store   = c.String("store-type")
		path    = c.String("store-path")
		carPath = c.String("car")
	)

	var bs blockstore.Blockstore
	switch store {
	case "lmdb":
		log.Println("using lmdb blockstore")
		bs, err = lmdbbs.Open(path)
		if err != nil {
			return err
		}

	case "sqlite3":
		log.Println("using sqlite3 blockstore")
		bs, err = sqlite3bs.Open(path, sqlite3bs.Options{})
		if err != nil {
			return err
		}

	case "faster":
		log.Println("using faster blockstore")
		bs, err = fasterbs.Open(path, fasterbs.Options{})
		if err != nil {
			return err
		}

	case "boltdb":
		log.Println("using boltdb blockstore")
		bs, err = bbolt.Open(path, &bbolt.Options{
			NoSync: true,
		})
		if err != nil {
			return err
		}

	case "badger":
		log.Println("using badger blockstore")

		// prepare the blockstore.
		bdgOpt := badger.DefaultOptions
		bdgOpt.GcInterval = 0
		bdgOpt.Options = bdg.DefaultOptions("")
		bdgOpt.Options.SyncWrites = false
		bdgOpt.Options.Truncate = true
		bdgOpt.Options.DetectConflicts = false

		ds, err := badger.NewDatastore(path, &bdgOpt)
		if err != nil {
			return err
		}
		defer ds.Close()
		bs = blockstore.NewBlockstore(ds)

	case "pebble":
		log.Println("using pebble blockstore")

		cache := 512
		ds, err := pebbleds.NewDatastore(path, &pebble.Options{
			// Pebble has a single combined cache area and the write
			// buffers are taken from this too. Assign all available
			// memory allowance for cache.
			Cache: pebble.NewCache(int64(cache * 1024 * 1024)),
			// The size of memory table(as well as the write buffer).
			// Note, there may have more than two memory tables in the system.
			// MemTableStopWritesThreshold can be configured to avoid the memory abuse.
			MemTableSize: cache * 1024 * 1024 / 4,
			// The default compaction concurrency(1 thread),
			// Here use all available CPUs for faster compaction.
			MaxConcurrentCompactions: runtime.NumCPU(),
			// Per-level options. Options for at least one level must be specified. The
			// options for the last level are used for all subsequent levels.
			Levels: []pebble.LevelOptions{
				{TargetFileSize: 16 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.NoCompression},
			},
		})
		if err != nil {
			return err
		}
		defer ds.Close()
		bs = blockstore.NewBlockstore(ds)
	}

	// close the blockstore if it supports closing (sqlite3 does).
	if cl, ok := bs.(io.Closer); ok {
		defer cl.Close()
	}

	var manifest *Manifest
	if c.Bool("import") {
		// do the import; this will generate a manifest which we pipe through to
		// the read task.
		manifest, err = doImport(c, bs, carPath)
		if err != nil {
			return fmt.Errorf("import failed: %w", err)
		}
	}

	// GC before starting the read.
	runtime.GC()
	runtime.GC()
	runtime.GC()

	if c.Bool("read") {
		// do the read; if manifest is nil, it means we haven't run the import
		// task in this execution. Expect to find a manifest alongside the CAR.
		if manifest == nil {
			// no import took place in this run.
			manifestFile := fmt.Sprintf("%s.manifest", carPath)
			f, err := os.Open(manifestFile)
			if err != nil {
				return fmt.Errorf("read: failed while opening manifest: %w", err)
			}
			manifest = &Manifest{File: f}
		}

		err := doRead(c, bs, manifest)
		if err != nil {
			return fmt.Errorf("read failed: %w", err)
		}
	}
	return nil
}

func doImport(c *cli.Context, bs blockstore.Blockstore, carPath string) (*Manifest, error) {
	carFile, err := os.Open(carPath)
	if err != nil {
		return nil, fmt.Errorf("could not find or open car: %s", err)
	}

	log.Printf("import: starting import")

	// wrap the input blockstore in a LimitBlockstore if a count of blocks to
	// import was supplied.
	var limit float64
	if lmt := c.String("import-limit"); len(lmt) > 0 {
		limit, _, err = humanize.ParseSI(lmt)
		if err != nil {
			return nil, fmt.Errorf("count parameter is invalid: %w", err)
		}
		log.Printf("import: limiting the import to %d blocks", int64(limit))
	}

	// wrap in limit blockstore; if zero, no limit will be applied.
	bs = LimitBlockstore(bs, int64(limit))

	manifestPath := fmt.Sprintf("%s.manifest", carPath)
	mbs := ManifestBlockstore(bs, manifestPath)

	now := time.Now()
	_, err = car.LoadCar(mbs, carFile)
	if err != nil && err != ErrLimitReached {
		return nil, fmt.Errorf("import failed: %w", err)
	}

	log.Printf("import: completed in %s", time.Since(now))

	return mbs.Manifest(), nil
}

func doRead(c *cli.Context, bs blockstore.Blockstore, m *Manifest) error {
	var (
		readCount    = c.String("read-count")
		repeatWindow = c.Int64("read-repeat-window")
		repeatRate   = c.Float64("read-repeat-rate")
	)

	cnt := m.Count()
	log.Printf("read: entries in manifest: %d", cnt)

	if readCount != "" {
		limit, _, err := humanize.ParseSI(readCount)
		if err != nil {
			return fmt.Errorf("read: count parameter is invalid: %w", err)
		}
		log.Printf("read: number of entries to read: %d", uint64(limit))
		cnt = uint64(limit)
	}

	cidCh, errCh := m.Get(cnt, repeatRate, repeatWindow)

	now := time.Now()
	var i uint64
	for c := range cidCh {
		if _, err := bs.Get(c); err != nil {
			return fmt.Errorf("read: blockstore get returned an error: %w", err)
		}
		i++
		if i%100000 == 0 {
			log.Printf("read: completed=%d/%d, remaining=%d/%d", i, cnt, cnt-i, cnt)
		}
	}

	log.Printf("read: completed in %s", time.Since(now))

	if err := <-errCh; err != nil {
		return fmt.Errorf("read: finished with error: %w", err)
	}

	log.Printf("read: finished successfully")

	return nil
}
