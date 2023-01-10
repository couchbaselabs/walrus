//  Copyright (c) 2022 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package walrus

import (
	"errors"
	"expvar"
	"fmt"
	"os"
	"regexp"
	"sync"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/google/uuid"
)

const (
	defaultCollectionName = "_default"
	defaultScopeName      = "_default"
	defaultCollectionID   = 0
)

var scopeCollectionNameRegexp = regexp.MustCompile("^[a-zA-Z0-9-][a-zA-Z0-9%_-]{0,250}$")

var (
	_ sgbucket.BucketStore            = &CollectionBucket{}
	_ sgbucket.DynamicDataStoreBucket = &CollectionBucket{}
	_ sgbucket.DeleteableStore        = &CollectionBucket{}
)

// CollectionBucket is an in-memory implementation of a BucketStore.  Individual
// collections are implemented as standard WalrusBuckets (in-memory DataStore implementations).
type CollectionBucket struct {
	path             string
	name             string
	uuid             string
	collectionIDs    map[scopeAndCollection]uint32 // collectionID by scope and collection name
	collections      map[uint32]*WalrusCollection  // WalrusCollection by collectionID
	lastCollectionID uint32                        // lastCollectionID assigned, used for collectionID generation
	lock             sync.Mutex                    // mutex for synchronized access to CollectionBucket
}

// A WalrusCollection wraps a walrus single key value store to add metadata
type WalrusCollection struct {
	*WalrusBucket
	FQName       scopeAndCollection // Fully qualified collection name (scope and collection)
	CollectionID uint32             // Unique collectionID
}

var _ sgbucket.DataStore = &WalrusCollection{}
var _ sgbucket.DataStoreName = &WalrusCollection{}

func (wh *WalrusCollection) ScopeName() string {
	return wh.FQName.ScopeName()
}

func (wh *WalrusCollection) CollectionName() string {
	return wh.FQName.CollectionName()
}

var collectionBuckets map[[2]string]*CollectionBucket
var collectionBucketsLock sync.Mutex

func GetCollectionBucket(url, bucketName string) (*CollectionBucket, error) {

	collectionBucketsLock.Lock()
	defer collectionBucketsLock.Unlock()
	if collectionBuckets == nil {
		collectionBuckets = make(map[[2]string]*CollectionBucket)
	}

	key := [2]string{url, bucketName}
	cb, ok := collectionBuckets[key]
	if ok {
		return cb, nil
	}

	cb = NewCollectionBucket(bucketName)
	dir := bucketURLToDir(url)
	if dir != "" {
		// TODO: persistence handling
		cb.path = dir
	}
	collectionBuckets[key] = cb
	return cb, nil
}

func NewCollectionBucket(bucketName string) *CollectionBucket {
	return &CollectionBucket{
		name:          bucketName,
		collections:   make(map[uint32]*WalrusCollection),
		collectionIDs: make(map[scopeAndCollection]uint32),
		uuid:          uuid.New().String(),
	}
}

func (wh *CollectionBucket) GetName() string {
	return wh.name
}

func (wh *CollectionBucket) UUID() (string, error) {
	return wh.uuid, nil
}

func (cb *CollectionBucket) Close() {
	collectionBucketsLock.Lock()
	defer collectionBucketsLock.Unlock()
	for key, value := range collectionBuckets {
		if value == cb {
			delete(collectionBuckets, key)
			break
		}
	}

	// Close all associated data stores
	cb.lock.Lock()
	defer cb.lock.Unlock()

	if cb.path != "" {
		// TODO: persist before closing
	}

	for _, store := range cb.collections {
		store.Close()
	}

}

func (wh *CollectionBucket) CloseAndDelete() error {
	path := wh.path
	wh.Close()
	if path == "" {
		return nil
	}
	return os.Remove(path)
}

func (wh *CollectionBucket) IsSupported(feature sgbucket.BucketStoreFeature) bool {
	switch feature {
	case sgbucket.BucketStoreFeatureCollections:
		return true
	case sgbucket.BucketStoreFeatureSubdocOperations:
		return false
	case sgbucket.BucketStoreFeatureXattrs:
		return false
	case sgbucket.BucketStoreFeatureN1ql:
		return false
	case sgbucket.BucketStoreFeatureCrc32cMacroExpansion:
		return false
	default:
		return false
	}
}

func (wh *CollectionBucket) DefaultDataStore() sgbucket.DataStore {
	return wh.getOrCreateCollection(scopeAndCollection{defaultScopeName, defaultCollectionName})
}

func (wh *CollectionBucket) NamedDataStore(name sgbucket.DataStoreName) (sgbucket.DataStore, error) {
	sc, err := newValidScopeAndCollection(name.ScopeName(), name.CollectionName())
	if err != nil {
		return nil, fmt.Errorf("attempting to create/update database with a scope/collection that is &s", err)
	}
	return wh.getOrCreateCollection(sc), nil
}

func (wh *CollectionBucket) CreateDataStore(name sgbucket.DataStoreName) error {
	sc, err := newValidScopeAndCollection(name.ScopeName(), name.CollectionName())
	if err != nil {
		return err
	}
	_ = wh.createCollection(sc)
	return nil
}

func (wh *CollectionBucket) DropDataStore(name sgbucket.DataStoreName) error {
	// intentionally not validating scope/collection name on drop, we'll either find a matching one or not.
	return wh.dropCollection(scopeAndCollection{name.ScopeName(), name.CollectionName()})
}

func (wh *CollectionBucket) ListDataStores() ([]sgbucket.DataStoreName, error) {
	wh.lock.Lock()
	defer wh.lock.Unlock()

	var result []sgbucket.DataStoreName
	for name := range wh.collectionIDs {
		result = append(result, name)
	}

	return result, nil
}

func (wh *CollectionBucket) GetMaxVbno() (uint16, error) {
	return 1024, nil
}
func (wh *CollectionBucket) StartTapFeed(args sgbucket.FeedArguments, dbStats *expvar.Map) (sgbucket.MutationFeed, error) {
	return nil, errors.New("Collection-aware bucket doesn't support tap feed, use DCP")
}

// StartDCPFeed implements a multi-collection feed by calling StartDCPFeed for each
// requested collection.  Each collection's DCP feed runs its own goroutine, callback may be invoked
// concurrently by these goroutines.
func (wh *CollectionBucket) StartDCPFeed(args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map) error {
	wh.lock.Lock()
	defer wh.lock.Unlock()
	// If no scopes are specified, return feed for the default collection, if it exists
	if args.Scopes == nil || len(args.Scopes) == 0 {
		defaultCollection, ok := wh.collections[defaultCollectionID]
		if ok {
			return defaultCollection.StartDCPFeed(args, callback, dbStats)
		} else {
			return errors.New("No scopes specified in feed arguments, and default collection does not exist")
		}
	}

	// Validate requested collections exist before starting feeds
	requestedCollections := make([]*WalrusCollection, 0)
	for scopeName, collections := range args.Scopes {
		for _, collectionName := range collections {
			collectionID, err := wh._getCollectionID(scopeName, collectionName)
			if err != nil {
				return fmt.Errorf("DCPFeed args specified unknown collection: %s:%s", scopeName, collectionName)
			}
			requestedCollections = append(requestedCollections, wh.collections[collectionID])
		}
	}

	doneChan := args.DoneChan
	doneChans := map[*WalrusCollection]chan struct{}{}
	for _, collection := range requestedCollections {
		// Not bothering to remove scopes from args for the single collection feeds
		// here because it's ignored by WalrusBucket's StartDCPFeed
		collectionID := collection.CollectionID
		collectionAwareCallback := func(event sgbucket.FeedEvent) bool {
			event.CollectionID = collectionID
			return callback(event)
		}

		// have each collection maintain its own doneChan
		doneChans[collection] = make(chan struct{})
		argsCopy := args
		argsCopy.DoneChan = doneChans[collection]

		// Ignoring error is safe because WalrusBucket doesn't have error scenarios for StartDCPFeed
		_ = collection.StartDCPFeed(argsCopy, collectionAwareCallback, dbStats)
	}

	// coalesce doneChans
	go func() {
		for _, collection := range requestedCollections {
			<-doneChans[collection]
		}
		close(doneChan)
	}()

	return nil
}

func (wh CollectionBucket) IsError(err error, errorType sgbucket.DataStoreErrorType) bool {
	if err == nil {
		return false
	}
	switch errorType {
	case sgbucket.KeyNotFoundError:
		_, ok := err.(sgbucket.MissingError)
		return ok
	default:
		return false
	}
}

func (wh CollectionBucket) GetCollectionID(scope, collection string) (uint32, error) {

	wh.lock.Lock()
	defer wh.lock.Unlock()
	return wh._getCollectionID(scope, collection)
}

func (wh CollectionBucket) _getCollectionID(scope, collection string) (uint32, error) {

	fqName, err := newValidScopeAndCollection(scope, collection)
	if err != nil {
		return 0, err
	}

	collectionID, ok := wh.collectionIDs[fqName]
	if !ok {
		return 0, sgbucket.MissingError{fqName.String()}
	}
	return collectionID, nil
}

func (wh *CollectionBucket) createCollection(name scopeAndCollection) sgbucket.DataStore {

	wh.lock.Lock()
	defer wh.lock.Unlock()

	return wh._createCollection(name)
}

func (wh *CollectionBucket) _createCollection(name scopeAndCollection) sgbucket.DataStore {

	collectionID := uint32(0)

	if !name.isDefault() {
		wh.lastCollectionID++
		collectionID = wh.lastCollectionID
	}

	dataStore := &WalrusCollection{
		FQName:       name,
		WalrusBucket: NewBucket(name.String()),
		CollectionID: collectionID,
	}

	wh.collections[collectionID] = dataStore
	wh.collectionIDs[name] = collectionID

	return dataStore
}

func (wh *CollectionBucket) getOrCreateCollection(name scopeAndCollection) sgbucket.DataStore {

	wh.lock.Lock()
	defer wh.lock.Unlock()

	collectionID, ok := wh.collectionIDs[name]
	if ok {
		return wh.collections[collectionID]
	}

	return wh._createCollection(name)
}

func (wh *CollectionBucket) dropCollection(name scopeAndCollection) error {

	if name.isDefault() {
		return errors.New("Default collection cannot be dropped")
	}

	wh.lock.Lock()
	defer wh.lock.Unlock()

	collectionID, ok := wh.collectionIDs[name]
	if !ok {
		return sgbucket.MissingError{Key: "collection not found"}
	}

	collection := wh.collections[collectionID]
	collection.Close()

	delete(wh.collections, collectionID)
	delete(wh.collectionIDs, name)
	return nil
}

// scopeAndCollection stores the (scope name, collection name) tuple for collectionID storage and retrieval
type scopeAndCollection struct {
	scope, collection string
}

var _ sgbucket.DataStoreName = &scopeAndCollection{"a", "b"}

func (sc scopeAndCollection) ScopeName() string {
	return sc.scope
}

func (sc scopeAndCollection) CollectionName() string {
	return sc.collection
}

func (sc scopeAndCollection) String() string {
	return sc.scope + ":" + sc.collection
}

func (sc scopeAndCollection) isDefault() bool {
	return sc.scope == defaultScopeName && sc.collection == defaultCollectionName
}

// newValidScopeAndCollection validates the names and creates new scope and collection pair
func newValidScopeAndCollection(scope, collection string) (id scopeAndCollection, err error) {
	if !isValidDataStoreName(scope, collection) {
		return id, errors.New("Invalid scope/collection name - only supports [A-Za-z0-9%-_]")
	}
	return scopeAndCollection{scope, collection}, nil
}

func isValidDataStoreName(scope, collection string) bool {
	if scope != defaultScopeName {
		return scopeCollectionNameRegexp.MatchString(scope) && scopeCollectionNameRegexp.MatchString(collection)
	}

	if collection != defaultCollectionName {
		return scopeCollectionNameRegexp.MatchString(collection)
	}

	return true
}
