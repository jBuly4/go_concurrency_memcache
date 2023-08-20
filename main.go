package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"go_concurrency_memcache/appsinstalled/appsinstalled"
	"google.golang.org/protobuf/proto"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var memcConnections = sync.Map{}

const (
	NORMAL_ERR_RATE = 0.01
	WORKERS         = 4
	THREADS         = 8
	BATCH           = 40000
)

// Options Struct for command-line options
type Options struct {
	test    bool
	logFile string
	dry     bool
	pattern string
	idfa    string
	gaid    string
	adid    string
	dvid    string
}

type AppsInstalled struct {
	DevType string
	DevID   string
	Lat     float64
	Lon     float64
	Apps    []uint32
}

func main() {
	opts := parseOptions()
	setupLogging(opts)

	if opts.test {
		protoTest()
		os.Exit(0)
	}

	log.Printf("Memc loader started with options: %+v\n", opts)

	mainFunc(opts)
}

func parseOptions() Options {
	opts := Options{}

	flag.BoolVar(&opts.test, "test", false, "Run tests")
	flag.StringVar(&opts.logFile, "log", "", "Log file path")
	flag.BoolVar(&opts.dry, "dry", false, "Dry run")
	flag.StringVar(&opts.pattern, "pattern", "./data/appsinstalled/*.tsv.gz", "Pattern for files to process")
	flag.StringVar(&opts.idfa, "idfa", "127.0.0.1:33013", "IDFA address")
	flag.StringVar(&opts.gaid, "gaid", "127.0.0.1:33014", "GAID address")
	flag.StringVar(&opts.adid, "adid", "127.0.0.1:33015", "ADID address")
	flag.StringVar(&opts.dvid, "dvid", "127.0.0.1:33016", "DVID address")

	flag.Parse()

	return opts
}

func setupLogging(opts Options) {
	if opts.logFile != "" {
		logFile, err := os.OpenFile(opts.logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("Error opening log file: %v", err)
		}
		log.SetOutput(logFile)
	}

	if opts.dry {
		log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
	} else {
		log.SetFlags(log.LstdFlags)
	}
}

func protoTest() {
	sample := "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
	for _, line := range strings.Split(sample, "\n") {
		parts := strings.Split(line, "\t")
		if len(parts) < 5 {
			log.Println("Invalid line format:", line)
			continue
		}

		//devType := parts[0]
		//devID := parts[1]
		lat, err1 := strconv.ParseFloat(parts[2], 64)
		lon, err2 := strconv.ParseFloat(parts[3], 64)
		if err1 != nil || err2 != nil {
			log.Println("Failed to parse lat or lon:", line)
			continue
		}

		rawApps := strings.Split(parts[4], ",")
		apps := make([]uint32, 0, len(rawApps))
		for _, app := range rawApps {
			if appID, err := strconv.Atoi(app); err == nil {
				apps = append(apps, uint32(appID))
			}
		}

		ua := &appsinstalled.UserApps{
			Lat:  &lat,
			Lon:  &lon,
			Apps: apps,
		}

		packed, err := proto.Marshal(ua)
		if err != nil {
			log.Fatalf("Failed to encode UserApps: %v", err)
		}

		unpacked := &appsinstalled.UserApps{}
		err = proto.Unmarshal(packed, unpacked)
		if err != nil {
			log.Fatalf("Failed to decode UserApps: %v", err)
		}

		if !proto.Equal(ua, unpacked) {
			log.Fatalf("Original and unpacked UserApps messages are not equal!")
		}
	}
}

func dotRename(path string) error {
	head, fn := filepath.Split(path)
	newPath := filepath.Join(head, "."+fn)
	return os.Rename(path, newPath)
}

func serialize(appsInstalled AppsInstalled) (string, []byte, error) {
	ua := &appsinstalled.UserApps{
		Lat:  &appsInstalled.Lat,
		Lon:  &appsInstalled.Lon,
		Apps: appsInstalled.Apps,
	}
	key := fmt.Sprintf("%s:%s", appsInstalled.DevType, appsInstalled.DevID)
	packed, err := proto.Marshal(ua)
	if err != nil {
		return "", nil, fmt.Errorf("failed to encode UserApps: %v", err)
	}
	return key, packed, nil
}

func insertAppsInstalled(memcAddr string, values map[string][]byte, dryRun bool) bool {
	// Check if we already have a connection for this address
	conn, ok := memcConnections.Load(memcAddr)
	if !ok {
		conn = memcache.New(memcAddr)
		memcConnections.Store(memcAddr, conn)
	}

	client := conn.(*memcache.Client)

	if dryRun {
		log.Printf("Dry run: %s - %v", memcAddr, values)
		return true
	}

	// Convert map of values to []*memcache.Item
	items := make([]*memcache.Item, 0, len(values))
	for k, v := range values {
		items = append(items, &memcache.Item{Key: k, Value: v})
	}

	// Iterate over map of values and set each item in Memcached
	for k, v := range values {
		err := client.Set(&memcache.Item{Key: k, Value: v})
		if err != nil {
			if errors.Is(err, memcache.ErrServerError) {
				log.Printf("Cannot write to memc %s: %s", memcAddr, err)
				memcConnections.Delete(memcAddr)
				return false
			}
			log.Printf("Error setting value in memc %s: %s", memcAddr, err)
			return false
		}
	}

	return true
}

func parseAppsInstalled(line string) *AppsInstalled {
	lineParts := strings.Split(strings.TrimSpace(line), "\t")
	if len(lineParts) < 5 {
		return nil
	}

	devType, devID, latStr, lonStr, rawApps := lineParts[0], lineParts[1], lineParts[2], lineParts[3], lineParts[4]
	if devType == "" || devID == "" {
		return nil
	}

	appsStr := strings.Split(rawApps, ",")
	var apps []uint32
	for _, appStr := range appsStr {
		app, err := strconv.Atoi(strings.TrimSpace(appStr))
		if err != nil {
			log.Printf("Not all user apps are digits: `%s`", line)
			continue
		}
		apps = append(apps, uint32(app))
	}

	lat, err := strconv.ParseFloat(latStr, 64)
	if err != nil {
		log.Printf("Invalid geo coords: `%s`", line)
		return nil
	}

	lon, err := strconv.ParseFloat(lonStr, 64)
	if err != nil {
		log.Printf("Invalid geo coords: `%s`", line)
		return nil
	}

	return &AppsInstalled{
		DevType: devType,
		DevID:   devID,
		Lat:     lat,
		Lon:     lon,
		Apps:    apps,
	}
}

func threadings(lines []string, deviceMemc map[string]string, opts Options, errors, processed *int32) {
	devices := make(map[string]map[string][]byte)

	for _, line := range lines {
		line = strings.TrimSpace(line)

		if line == "" {
			continue
		}

		apps := parseAppsInstalled(line)
		if apps == nil {
			atomic.AddInt32(errors, 1)
			continue
		}

		memcAddr, ok := deviceMemc[apps.DevType]
		if !ok {
			log.Printf("Unknown device type: %s", apps.DevType)
			continue
		}

		key, packed, _ := serialize(*apps)

		if _, exists := devices[memcAddr]; !exists {
			devices[memcAddr] = make(map[string][]byte)
		}
		devices[memcAddr][key] = packed
	}

	for memcAddr, values := range devices {
		inserted := insertAppsInstalled(memcAddr, values, opts.dry)
		if inserted {
			atomic.AddInt32(processed, 1)
		} else {
			atomic.AddInt32(errors, 1)
		}
	}
}

func printErrorState(errors, processed int32) {
	errRate := float64(errors) / float64(processed)
	if errRate < NORMAL_ERR_RATE {
		log.Printf("Acceptable error rate %f", errRate)
	} else {
		log.Printf("High error rate %f > %f. Failed Load", errRate, NORMAL_ERR_RATE)
	}
}

func processFile(fn string, deviceMemc map[string]string, opts Options, errors, processed *int32) {
	log.Printf("Processing %s", fn)

	file, err := os.Open(fn)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	fd, err := gzip.NewReader(file)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer fd.Close()

	var batch []string
	var wg sync.WaitGroup

	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		line := scanner.Text()
		batch = append(batch, line)
		if len(batch) == BATCH {
			wg.Add(1)
			go func(b []string) {
				defer wg.Done()
				threadings(b, deviceMemc, opts, errors, processed)
			}(batch)
			batch = []string{}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading from the gzipped file: %v", err)
	}

	if len(batch) > 0 {
		wg.Add(1)
		go func(b []string) {
			defer wg.Done()
			threadings(b, deviceMemc, opts, errors, processed)
		}(batch)
	}

	wg.Wait()

	if processed == nil {
		dotRename(fn)
		return
	}

	printErrorState(*errors, *processed)
	dotRename(fn)
}

func mainFunc(opts Options) {
	deviceMemc := map[string]string{
		"idfa": opts.idfa,
		"gaid": opts.gaid,
		"adid": opts.adid,
		"dvid": opts.dvid,
	}

	var errors, processed int32
	files, _ := filepath.Glob(opts.pattern)

	var wg sync.WaitGroup
	for _, fn := range files {
		wg.Add(1)
		go func(filename string) {
			defer wg.Done()
			processFile(filename, deviceMemc, opts, &errors, &processed)
		}(fn)
	}
	wg.Wait()
}
