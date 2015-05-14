package walrus

import (
	"encoding/gob"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/couchbase/sg-bucket"
)

// How long to wait after an in-memory change before saving to disk
const kSaveInterval = 2 * time.Second

func (bucket *lolrus) _save() error {
	if bucket.path == "" {
		return nil
	}
	file, err := ioutil.TempFile(filepath.Dir(bucket.path), "walrustemp")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())

	encoder := gob.NewEncoder(file)
	encoder.Encode(bucket.lolrusData)
	file.Close()

	err = os.Rename(file.Name(), bucket.path)
	if err == nil {
		bucket.lastSeqSaved = bucket.LastSeq
	}
	return err
}

func load(path string) (*lolrus, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	bucket := &lolrus{
		path:  path,
		views: map[string]lolrusDesignDoc{},
	}
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&bucket.lolrusData)
	if err != nil {
		log.Printf("Decode error: %v", err)
		return nil, err
	}
	bucket.lastSeqSaved = bucket.LastSeq

	// Recompile the design docs:
	for name, ddoc := range bucket.DesignDocs {
		if err := bucket._compileDesignDoc(name, ddoc); err != nil {
			return nil, err
		}
	}
	runtime.SetFinalizer(bucket, (*lolrus).Close)
	log.Printf("Loaded bucket from %s", path)
	return bucket, nil
}

func loadOrNew(path string, name string) (*lolrus, error) {
	bucket, err := load(path)
	if os.IsNotExist(err) {
		bucket = NewBucket(name).(*lolrus)
		bucket.path = path
		log.Printf("New bucket for new path %s", path)
		return bucket, nil
	}
	return bucket, err
}

// Schedules a save for the near future. MUST be called while holding a write lock!
func (bucket *lolrus) _saveSoon() {
	if !bucket.saving && bucket.path != "" {
		bucket.saving = true
		go func() {
			// Spin off a goroutine to wait and then save:
			time.Sleep(kSaveInterval)

			bucket.lock.Lock()
			defer bucket.lock.Unlock()
			if bucket.saving {
				bucket.saving = false
				log.Printf("Saving bucket to %s", bucket.path)
				if err := bucket._save(); err != nil {
					log.Printf("Walrus: Warning: Couldn't save walrus bucket: %v", err)
				}
			}
		}()
	}
}

// Loads or creates a persistent bucket in the given filesystem directory.
// The bucket's backing file will be named "bucketName.walrus", or if the poolName is not
// empty "default", "poolName-bucketName.walrus".
func NewPersistentBucket(dir, poolName, bucketName string) (sgbucket.Bucket, error) {
	filename := bucketName + ".walrus"
	if poolName != "" && poolName != "default" {
		filename = poolName + "-" + filename
	}
	bucket, err := loadOrNew(filepath.Join(dir, filename), bucketName)
	if err != nil {
		return nil, err
	}
	bucket.name = bucketName
	return bucket, nil
}

func (bucket *lolrus) _closePersist() error {
	if !bucket.saving {
		return nil
	}
	bucket.saving = false // defuse pending save goroutine (see _saveSoon)
	return bucket._save()
}

func (bucket *lolrus) isSequenceSaved(seq uint64) bool {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()
	return bucket.lastSeqSaved >= seq
}
