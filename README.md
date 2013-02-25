# Walrus

<img src="http://www.ihasabucket.com/images/walrus_bucket.jpg">

**Walrus** is a [Go](http://golang.org) library that provides a tiny implementation of the Bucket API from the [go-couchbase](http://github.com/couchbaselabs/go-couchbase) package.

If you have Go code that talks to a Couchbase Server bucket, you can fairly easily switch it to use an in-process Walrus bucket instead. This is handy for development and testing, since you don't have to run a full Couchbase Server instance, configure buckets, flush them before running tests, etc.

Walrus is not fast or scalable or even persistent. It's not supposed to be. It's supposed to be the simplest thing that could possibly implement the Bucket API.

## Walrus supports

* Basic CRUD operations on both JavaScript and raw documents
* Atomic increment
* Design documents and views with JavaScript map functions
* Key ranges and limits in queries

## Walrus does not yet support

* Direct CAS operations (only the higher-level Update call)
* Document expiration
* Reduce functions in views
* Offsets or reverse order in queries

Patches gratefully accepted :)

## Walrus intentionally does not support

* Network access -- it's a library, not a server
* Data persistence -- it's RAM-only

## Using Walrus

It couldn't be easier:

	import "github.com/couchbaselabs/walrus"

	...

	bucket := walrus.NewBucket("bucketname")

Now you have an empty bucket you can use as though it were a [`couchbase.Bucket`](http://godoc.org/github.com/couchbaselabs/go-couchbase#Bucket).
