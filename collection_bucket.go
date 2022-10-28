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

var defaultScopeAndCollection = scopeAndCollection{"_default", "_default"}

var scopeCollectionNameRegexp = regexp.MustCompile("^[a-zA-Z0-9-][a-zA-Z0-9%_-]{0,250}$")

var _ sgbucket.BucketStore = &CollectionBucket{}

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

var _ sgbucket.BucketStore = &CollectionBucket{}

func GetCollectionBucket(url, bucketName string) (*CollectionBucket, error) {

	cb := NewCollectionBucket(bucketName)
	dir := bucketURLToDir(url)
	if dir != "" {
		// TODO: persistence handling
		cb.path = dir
	}
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

func (wh *CollectionBucket) Close() {
	// Close all associated data stores
	wh.lock.Lock()
	defer wh.lock.Unlock()

	if wh.path != "" {
		// TODO: persist before closing
	}

	for _, store := range wh.collections {
		store.Close()
	}
}

func (wh *CollectionBucket) IsSupported(feature sgbucket.BucketStoreFeature) bool {
	switch feature {
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
	return wh.NamedDataStore(defaultScopeName, defaultCollectionName)
}

func (wh *CollectionBucket) NamedDataStore(scope, collection string) sgbucket.DataStore {

	sc, err := newScopeAndCollection(scope, collection)
	if err != nil {
		return nil
	}
	return wh.getOrCreateCollection(sc)
}

func (wh *CollectionBucket) DropDataStore(scope, collection string) error {
	sc, err := newScopeAndCollection(scope, collection)
	if err != nil {
		return err
	}
	return wh.dropCollection(sc)
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

	for _, collection := range requestedCollections {
		// Not bothering to remove scopes from args for the single collection feeds
		// here because it's ignored by WalrusBucket's StartDCPFeed
		collectionID := collection.CollectionID
		collectionAwareCallback := func(event sgbucket.FeedEvent) bool {
			event.CollectionID = collectionID
			return callback(event)
		}
		// Ignoring error is safe because WalrusBucket doesn't have error scenarios for StartDCPFeed
		_ = collection.StartDCPFeed(args, collectionAwareCallback, dbStats)
	}

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

	fqName, err := newScopeAndCollection(scope, collection)
	if err != nil {
		return 0, err
	}

	collectionID, ok := wh.collectionIDs[fqName]
	if !ok {
		return 0, sgbucket.MissingError{fqName.String()}
	}
	return collectionID, nil
}

func (wh *CollectionBucket) getOrCreateCollection(name scopeAndCollection) sgbucket.DataStore {

	wh.lock.Lock()
	defer wh.lock.Unlock()

	collectionID, ok := wh.collectionIDs[name]
	if ok {
		return wh.collections[collectionID]
	}

	collectionID = 0
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
type scopeAndCollection [2]string

func (sc scopeAndCollection) String() string {
	return sc[0] + ":" + sc[1]
}

func (sc scopeAndCollection) isDefault() bool {
	return sc[0] == defaultScopeName && sc[1] == defaultCollectionName
}

// Creates new scope and collection pair, with name validation (required for uniqueness of String())
func newScopeAndCollection(scope, collection string) (id scopeAndCollection, err error) {
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
