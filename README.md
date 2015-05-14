[![Build Status](https://drone.io/github.com/couchbaselabs/walrus/status.png)](https://drone.io/github.com/couchbaselabs/walrus/latest)
[![GoDoc](https://godoc.org/github.com/couchbaselabs/walrus?status.png)](https://godoc.org/github.com/couchbaselabs/walrus)

# Walrus

<img src="http://www.ihasabucket.com/images/walrus_bucket.jpg">

**Walrus** is a [Go](http://golang.org) library that provides a tiny implementation of the Bucket API from the [go-couchbase](http://github.com/couchbaselabs/go-couchbase) package.

If you have Go code that talks to a Couchbase Server bucket, you can fairly easily switch it to use an in-process Walrus bucket instead. This is handy for development and testing, since you don't have to run a full Couchbase Server instance, configure buckets, flush them before running tests, etc.

Walrus is not fast or scalable. It's not supposed to be. It's supposed to be the simplest thing that could possibly implement the Bucket API.

## Walrus supports

* Basic CRUD operations on both JavaScript and raw documents
* Atomic increment
* Design documents and views with JavaScript map functions
* Key ranges and limits in queries
* Persistence (by archiving the in-memory data structures to a file)

## Walrus does not yet support

* Direct CAS operations (only the higher-level Update call)
* Document expiration
* Reduce functions in views
* Offsets or reverse order in queries

Patches gratefully accepted :)

## Walrus intentionally does not support

* Network access -- it's a library, not a server
* Incremental updates of the persistent store
* Multiple processes sharing a persistent store

## Using Walrus

It couldn't be easier:

	import "github.com/couchbaselabs/walrus"

Now you can change any of your `couchbase.GetBucket` calls to the equivalent Walrus call:

	bucket := walrus.GetBucket("http://localhost:8091", "default", "bucketName")

This creates an in-memory (non-persistent) bucket you can use as though it were a [`couchbase.Bucket`](http://godoc.org/github.com/couchbaselabs/go-couchbase#Bucket). The HTTP URL and pool name are ignored, except that if you call `GetBucket` with the same three parameter values again, it will return the same `Bucket` instance.

In the simplest case, if you just want to create a new non-persistent bucket, you can just call:

	bucket := walrus.NewBucket("bucketname")

### Persistent buckets

You can create a persistent bucket by using a "file:" or "walrus:" URL, or just an absolute directory path. Any of these three calls will create a persistent bucket, whose backing file will be `/tmp/bucketName.walrus`:

	walrus.GetBucket("file:///tmp", "default", "bucketName")
	walrus.GetBucket("walrus:/tmp", "default", "bucketName")
	walrus.GetBucket("/tmp", "default", "bucketName")

Buckets persist themselves simply by archiving the Bucket object to a data blob using the Gob format, then writing that blob to a file. When a bucket's contents are changed, it schedules a save for two seconds in the future, which will (atomically) rewrite the entire file. Again, this is obviously not scalable, but it's simple and works fine for small data sets.
