<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  
   http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Output Streams


With the exception of `FileSystem.copyFromLocalFile()`, 
all API methods operations which write data to a filesystem is done
through the Java "OutputStreams" API.
Specifically, through `OutputStream` subclasses obtained through calls to
`FileSystem.create()`, `FileSystem.append()`,
or `FSDataOutputStreamBuilder.build()`.

These all return instances of `FSDataOutputStream`, through which data 
can be written through various `write()` methods.
After a stream's `close()` method is called, all data written to the 
stream MUST BE persisted to the fileysystem and visible to oll other
clients attempting to read data from that path via `FileSystem.open()`
—until any change happens to that data, such as it being overwritten, deleted,
or appended to.

As well as operations to write the data, Hadoop's Output Streams
provide methods to flush buffered data back to the filesystem,
so as to ensure that the data is reliably persisted and/or visible
to other callers. This is done via the `Syncable` interface.


## Issues with the Hadoop Output Stream model.


There are some known issues with the output stream model as offered by Hadoop,
specifically about the guarantees about when data is written and persisted  
—and when the metadata is synchronized.
These are where implementation aspects of HDFS and the "Local" filesystem
do not the simple model of the filesystem used in this specification.

*HDFS*: The metadata on a file, specifically its length and modification time, may lag
that of the actual data. That is: if a file is opened and read, more data may
be returned than a call to `getFileStatus()` would indicate. It is only
once an output stream has been closed that the metadata and data can be guaranteed
to be consistent.

*Local* `flush()` does not guaranteed to flush all the data to the local filesystem.
This is because the checksum process needs whole checksummable chunks to scan
to produce a checksum: only complete chunks are flushed, When the output stream
is closed via `close()`, then all data is written, even any trailing incomplete chunk.


Object stores have their own behaviors. some of which may differs significantly
from that of a classic POSIX filesystem.

1. There is no guarantee that any file will be visible at the path of an output
stream after the output stream is created.

1. There is no guarantee that any file or data will be visible at the path of an output
stream after data is written to the output stream and it is then flushed by
any of the flush/sync operations supported in the stream's (static) interfaces.

1. Calls to `create(FS, Path, overwrite=false)` may succeed, returning a new
`OutputStream`, even while another stream is open and writing to the destination
path.

One guarantee which Object Stores SHOULD make is the same as those of POSIX
filesystems: After a stream `close()` call returns, the data must be persisted
durably and visible to all callers. Unfortunately, even that guarantee is
not always met:

1. Existing data on a path MAY be visible for an indeterminate period of time. 

1. If the store has any form of create inconsistency or buffering of negative
existence probes, then even after the stream's `close()` operation has returned,
`getFileStatus(FS, path)` and `open(FS, path)` may fail with a `FileNotFoundException`.

In their favour, the atomicity of the store's PUT operations do offer their
own guarantee: an newly created object is either absent or all of its data
is present: the act of instantiatng the object, while potentially exhibiting
create inconsistency, is atomic. Applications may be able to use that fact
to their advantage.    

To exacerbate the problem, the interface whose existence is meant to
offer guarantees of durability, `Syncable`, is declared as an implementation
of all `FSDataOutputStream` instances: it is impossible to use
`instanceof Syncable` as a check for the durability semantics.

This is precisely why the `StreamCapabilities` interface has been added
to streams in [HDFS-11644](https://issues.apache.org/jira/browse/HDFS-11644);
its semantics are also specified here.



## Output stream model

An output stream consists of a buffer `buffer: List[byte]`

For this specification, the output stream can be viewed as a list of bytes

```python
buffer: List[byte]`
```

the buffer, bonded to a path in its owning filesystem. A flag, `open`
tracks whether the stream is open: after the stream is closed no more
data may be written to it

```python
FS: FileSystem
path: List[PathElement]
open: bool
buffer: List[byte]
```


(Immediately) after `Syncable` operations which flush data to the filesystem, 
the data at the stream's destination path must match that of
`buffer`. That is, the following condition holds
 

```python
FS'.Files(path) == buffer
```

   
data is flushed, read operations on the filesystem
should be in sync with the data

```python
Stream' = (FS', path, open, buffer)
```


### State of Stream and filesystem after `Filesystem.create()`


The output stream returned by a `FileSystem.create(path)` call must be empty

```python
Stream' = (FS, path, true, [])
```


The filesystem `FS'` must contain a 0-byte file at the path:
 
```python
data(FS', path) == []
```

Accordingly, the the initial state of `Stream'.buffer` is implicitly
consistent with the data at the filesystem.


### State of Stream and filesystem after `Filesystem.append()`

The output stream returned from a call of
 `FileSystem.append(path, buffersize, progress)`,
can be modelled as a stream whose `buffer` is intialized to that of
the original file:

```python
Stream' = (FS, path, true, data(FS, path))
FS ' = FS
```


Again, the the initial state of `Stream'.buffer` is implicitly
consistent with the data at the filesystem.


####  Persisting data to the Filesystem

When the stream writes data back to the Filesystem, be it in any 
supported flush operation, in the `close()` operation, or at any other
time the stream chooses to do so, the contents of the file
are replaced with the current buffer


```python
    Stream'=(FS', path, true, buffer)
    FS' = FS where data(FS', path) == buffer
```


After a call to `close()`, the stream is closed for all operations other
than `close()`; they MAY fail with `IOException` or `RuntimeException` instances.

```python
Stream' =  (FS, path, false, [])
```

The `close()` operation must become a no-op. That is: subsequent attempts to
persist data after a failure MUST NOT be made. (alternatively: users of the
API MUST NOT expect failing `close()` attemps to succeed if retried) 




<!--  ============================================================= -->
<!--  CLASS: FSDataOutputStream -->
<!--  ============================================================= -->


# Class `FSDataOutputStream`


```java
public class FSDataOutputStream
  extends DataOutputStream
  implements Syncable, CanSetDropBehind, StreamCapabilities {
 // ...
 }
```

The `FileSystem.create()`, `FileSystem.append()` and
`FSDataOutputStreamBuilder.build()` calls return an instance
of a class `FSDataOutputStream`, a subclass of `java.io.OutputStream`.

The base class wraps an `OutputStream` instance, one which may implement `Streamable`,
`CanSetDropBehind` and `StreamCapabilities`.

This document covers the requirements of such implementations.

HDFS's `FileSystem` implementation, `DistributedFileSystem`, returns an instance
of `HdfsDataOutputStream`. This implementation has at least two behaviors
which are not explicitly declared by the base Java implmentation

1. Writes are synchronized: more than one thread can write to the same
output stream. This is a use pattern which HBase relies on.

1. `OutputStream.flush()` is a no-op when the file is closed. Apache Druid
has made such a call on this in the past
[HADOOP-14346](https://issues.apache.org/jira/browse/HADOOP-14346).
 

As the HDFS implementation is considered the de-facto specification of
the FileSystem APIs, the fact that `write()` is thread-safe is significant,
because programs rely on this.

For compatibility, not only must other FS clients be thread-safe,
but new HDFS featues, such as encryption and Erasure Coding must also
implement consistent behavior with the core HDFS output stream.

Put differently

*it isn't enough for Output Streams to implement the core semantics
of `java.io.OutputStream`: they need to implement the extra semantics
of `HdfsDataOutputStream`. Failure to do so must be considered regressions*

The concurrent `write()` call is the most significant tightening of
this specification. 


## Class `java.io.OutputStream`


A Java `OutputStream` allows applications to write a sequence of bytes to a destination.
In a Hadoop filesystem, that destination is the data under a path in the filesystem.


```java
public abstract class OutputStream implements Closeable, Flushable {
  public abstract void write(int b) throws IOException;
  public void write(byte b[]) throws IOException;
  public void write(byte b[], int off, int len) throws IOException;
  public void flush() throws IOException ;
  public void close() throws IOException;
}
```

The 


### <a name="write(data)"></a>`write(Stream, data)`

#### Preconditions

```python
Stream.open else raise IOException
```

#### Postconditions

The buffer has the lower 8 bits of the data argument appended to it.

```python
Stream'.buffer = Stream.buffer + [data & 0xff]
```

There may be an explicit limit on the size of cached data, or an implicit
limit based by the available capacity of the destination filesystem.
When a limit is reached, `write()` SHOULD fail with an `IOException`.

### <a name="write(buffer,offset,len)"></a>`write(Stream, byte[] data, int offset, int len)`


#### Preconditions

The preconditions are all defined in `OutputStream.write()`

```python
Stream.open else raise IOException
data != null else raise NullPointerException
offset >= 0 else raise IndexOutOfBoundsException
len >= 0 else raise IndexOutOfBoundsException
offset < data.length else raise IndexOutOfBoundsException
offset + len < data.length else raise IndexOutOfBoundsException
```


There may be an explicit limit on the size of cached data, or an implicit
limit based by the available capacity of the destination filesystem.
When a limit is reached, `write()` SHOULD fail with an `IOException`.

After the operation has returned, the buffer may be re-used. The outcome
of updates to the buffer while the `write()` operation is in progress is undefined.

#### Postconditions

```python
Stream'.buffer' = Stream.buffer + data[offset...(offset + len)]
```



### <a name="write(buffer)"></a>`write(byte[] data)`

This is defined as the equivalent of

```python
write(data, 0, data.length)
```


### <a name="flush()"></a>`flush()`

Requests that the data is flushed. The specification of `ObjectStream.flush()`
declares that this SHOULD write data to the "intended destination".

It explicitly precludes any guarantees about durability.

For that reason, this document doesn't provide any normative
specifications of behaviour.

#### Preconditions


```python
Stream.open else raise IOException
```

#### Postconditions

 ```python
FS' = FS where data(path) == cache
 ```

Some applications have been known to call `flush()` on a closed stream
on the assumption that it is harmless. Implementations MAY
support this behaviour.

### <a name="close"></a>`close()`

The `close()` operation saves all data to the filesystem and
releases any resources used for writing data.
All subsequent `write()` calls on the stream MUST then fail with an `IOException`.

The `close()` call is expected to block
until the write has completed (as with `Syncable.hflush()`), possibly
until it has been written to durable storage (as HDFS does).

After `close()` completes, the data in a file MUST be visible and consistent
with the data most recently written. The metadata of the file MUST be consistent
with the data and the write history itself (i.e. any modification time fields
updated).

Any locking/leaseholding mechanism is also required to release its lock/lease.

```python
Stream'.open = false
FS' = FS where data(path) == cache
```

The `close()` call MAY fail during its operation.

1. Callers of the API MUST expect for some calls to fail and SHOULD code appropriately.
Catching and swallowing exceptions, while common, is not always the ideal solution.
1. Even after a failure, `close()` MUST place the stream into a closed state.
Follow-on calls to `close()` are ignored, and calls to other methods
rejected. That is: caller's cannot be expected to call `close()` repeatedly
until it succeeds.
1. The duration of the `call()` operation is undefined. Operations which rely
on acknowledgements from remote systems to meet the persistence guarantees
implicitly have to await these acknowledgements. Some Object Store output streams
upload the entire data file in the `close()` operation. This can take a large amount
of time. The fact that many user applications assume that `close()` is both fast
and does not fail means that this behavior is dangerous.

Recommendations for safe use by callers

* Do plan for exceptions being raised, either in catching and logging or
by throwing the exception further up. Catching and silently swallowing exceptions
may hide serious problems.
* Heartbeat operations SHOULD take place on a separate thread, so that a long
delay in `close()` does not block the thread so long that the heartbeat times
out.


## Interface `StreamCapabilities`

```java
@InterfaceAudience.Public
@InterfaceStability.Evolving
```

The `StreamCapabilities` interface exists to allow callers to dynamically
determine the behavior of a stream.


The reference Implementation of this interface is
 `org.apache.hadoop.hdfs.DFSOutputStream`

```java
public boolean hasCapability(String capability) {
  if (capability.equalsIgnoreCase(HSYNC.getValue()) ||
      capability.equalsIgnoreCase((HFLUSH.getValue()))) {
    return true;
  }
  return false;
}
```

Where `HSYNC` and `HFLUSH` are items in the enumeration 
`org.apache.hadoop.fs.StreamCapabilities.StreamCapability`.


## <a name="syncable"></a>`org.apache.hadoop.fs.Syncable`

```java
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface Syncable {
  /**
   * @deprecated As of HADOOP 0.21.0, replaced by hflush
   * @see #hflush()
   */
  @Deprecated void sync() throws IOException;
  
  /** Flush out the data in client's user buffer. After the return of
   * this call, new readers will see the data.
   * @throws IOException if any error occurs
   */
  void hflush() throws IOException;
  
  /** Similar to posix fsync, flush out the data in client's user buffer 
   * all the way to the disk device (but the disk may have it in its cache).
   * @throws IOException if error occurs
   */
  void hsync() throws IOException;
}
```

The purpose of `Syncable` interface is to provide guarantees that data is written
to a filesystem for both visibility and durability.

*SYNC-1*: An `OutputStream` which implements `Syncalbe` is 
making an explicit declaration of an that it can meet those guarantees.
meet those guarantees.

*SYNC-2*: The interface MUST NOT be declared as implemented by an `OutputStream` unless
those guarantees can be met.

The `Syncable` interface has been implemented by other classes than
subclasses of `OutputStream`, such as `org.apache.hadoop.io.SequenceFile.Writer`.

*SYNC-3* The fact that a class implements `Syncable` does not guarantee
that `extends OutputStream` holds.

That is, for any class `C`: `(C instanceof Syncable)` does not imply
`(C instanceof OutputStream)`

This specification only covers the required behavior of `OutputStream` subclasses
which implement `Syncable`.


*SYNC-4:* The return value of `FileSystem.create(Path)` is an instance
of `FSDataOutputStream`.

*SYNC-5:* `FSDataOutputStream implements Syncable`


SYNC-5 and SYNC-1 imply that all output streams which can be created
with `FileSystem.create(Path)` must support the semantics of `Syncable`.
Any inspection of the Hadoop codebase will make clear that this is demonstrably
not true, therefore these declarations SYNC-1 and SYNC-2 are
demonstrably false.

Put differently: *callers cannot rely on the presence of the interface
as evidence that the semantics of `Syncable` are supported*. Instead
they should be dynamically probed for using the `StreamCapabilities`
interface, where available.


### <a name="Syncable.hflush"></a>`Syncable.hflush()`

Flush out the data in client's user buffer. After the return of
this call, new readers will see the data. The `hflush()` operation
does contain guarantees as to the durability of the data. only
is visibility. 
Thus implementations may cache the written data in memory 
—visible to all, but not yet persisted.

#### Preconditions

```python
hasCapability(Stream. "hflush") else raise UnsupportedOperationException

Stream.open else raise IOException
```


#### Postconditions

```python
FS' = FS where data(path) == cache
```


After the call returns, the data MUST be visible to all new callers
of `FileSystem.open`.

There is no requirement or guarantee that clients with an existing
`DataInputStream` created by a call to `(FS, path)` will see the updated
data, nor is there a guarantee that they *will not* in a current or subsequent
read. 

Implementation note: as a correct `hsync()` implementation must also
offer all the semantics of an `hflush()` call, implementations of `hflush()`
may just invoke `hsync()`:

```java
public void hflush() throws IOException {
  hsync();
}
``` 

### <a name="Syncable.hsync"></a>`Syncable.hsync()`

Similar to POSIX `fsync`, this call saves the data in client's user buffer
all the way to the disk device (but the disk may have it in its cache).

That is: it is a requirement for the underlying FS To save all the data to
the disk hardware itself, where it is expected to be durable.

#### Preconditions

```python
hasCapability(Stream, "hsync") else raise UnsupportedOperationException
Stream.open else raise IOException
```

#### Postconditions

```python
FS' = FS where data(path) == buffer
```

The reference implementation, `DFSOutputStream` will block
until an acknowledgement is received from the datanodes: That is, all hosts
in the replica write chain have successfully written the file.

That means that the expectation callers may have is that the return of
the method call contains visibility and durability guarantees which other
implementations must maintain.

Note, however, that the reference `DFSOutputStream.hsync()` call only actually syncs/
*the current block*. If there have been a series of writes since the last sync,
such that a block boundary has been crossed. The `hsync()` call claims only
to write the most recent.


From the javadocs of `DFSOutputStream.hsync(EnumSet<SyncFlag> syncFlags)`

> Note that only the current block is flushed to the disk device.
> To guarantee durable sync across block boundaries the stream should
> be created with {@link CreateFlag#SYNC_BLOCK}.


In virtual machines, the notion of "disk hardware" is really that of
another software abstraction: there are guarantees.


## `interface CanSetDropBehind` 

```java
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface CanSetDropBehind {
  /**
   * Configure whether the stream should drop the cache.
   *
   * @param dropCache     Whether to drop the cache.  null means to use the
   *                      default value.
   * @throws IOException  If there was an error changing the dropBehind
   *                      setting.
   *         UnsupportedOperationException  If this stream doesn't support
   *                                        setting the drop-behind.
   */
  void setDropBehind(Boolean dropCache)
      throws IOException, UnsupportedOperationException;
}
```

This interface allows callers to change policies used inside HDFS. 
They are currently unimplemented by any stream other than those in HDFS. 



## Durability, Concurrency, Consistency and Visibility of stream output.

These are the aspects of the system behaviour which are not directly 
covered in this (very simplistic) Filesystem model, but which are visible
in production.


## Durability

1. `OutputStream.write()` MAY persist the data, synchronously or asynchronously
1. `OutputStream.flush()` flushes data to the destination. There
are no strict persistence requirements.
1. `Syncable.hflush()` synchronously sends all local data to the destination
filesystem. After returning to the caller, the data MUST be visible to other readers,
it MAY be durable. That is: it does not have to be persisted, merely guaranteed
to be consistently visible to all clients attempting to open a new stream reading
data at the path.
1. `Syncable.hsync()` MUST flush the data and persist data to the underlying durable
storage.
1. `close()` The first call to `close()` MUST flush out all remaining data in
the buffers, and persist it.


## Concurrency

1. The outcome of more than one process writing to the same file is undefined.

1. An input stream opened to read a file *before the file was opened for writing*
MAY fetch data updated by writes to an OutputStream.
Because of buffering and caching, this is not a requirement
—and if a input stream does pick up updated data, the point at
which the updated data is read is undefined. This surfaces in object stores
where a `seek()` call which closes and re-opens the connection may pick up
updated data, while forward stream reads do not. Similarly, in block-oriented
filesystems, the data may be cached a block at a time —and changes only picked
up when a different block is read.

1. A Filesystem MAY allow the destination path to be manipulated while a stream
is writing to it —for example, `rename()` of the path or a parent; `delete()` of
a path or parent. In such a case, the outcome of future write operations on
the output stream is undefined. Some filesystems MAY implement locking to
prevent conflict. However, this tends to be rare on distributed filesystems,
for reasons well known in the literature.

1. The Java API specification of `java.io.OutputStream` does not require
an instance of the class to be thread safe.
However, `org.apache.hadoop.hdfs.DFSOutputStream`
has a stronger thread safety model (possibly unintentionally). This fact is
relied upon in Apache HBase, as discovered in HADOOP-11708. Implementations
SHOULD be thread safe. *Note*: even the `DFSOutputStream` synchronization
model permits the output stream to `close()`'d while awaiting an acknowledgement
from datanode or namenode writes in an `hsync()` operation.


## Consistency and Visibility

There is no requirement for the data to be immediately visible to other applications
—not until a specific call to flush buffers or persist it to the underlying storage
medium are made.

1. If an output stream is created with `FileSystem.create(path)`, with `overwrite==true`
and there is an existing file at the path, that is `exists(FS, path)` holds,
then, the existing data is immediately unavailable; the data at the end of the
path MUST consist of an empty byte sequence `[]`, with consistent metadata.

1. The metadata of a file (`length(FS, path)` in particular) SHOULD be consistent
with the contents of the file after `flush()` and `sync()`.
HDFS does not do this except when the write crosses a block boundary; to do
otherwise would overload the Namenode. As a result, while a file is being written
`length(Filesystem, Path)` MAY be less than the length of `data(Filesystem, Path)`.

1. The metadata MUST be consistent with the contents of a file after the `close()`
operation.

1. If the filesystem supports modification timestamps, this
timestamp MAY be updated while a file is being written, especially after a
`Syncable.hsync()` call. The timestamps MUST be updated after the file is closed.

1. After the contents of an output stream have been persisted (`hflush()/hsync()`)
all new `open(FS, Path)` operations MUST return the updated data.

1. After `close()` has been invoked on an output stream,
a call to `getFileStatus(path)` MUST return the final metadata of the written file,
including length and modification time.
The metadata of the file returned in any of the FileSystem `list` operations
MUST be consistent with this metadata.

1. The value of `getFileStatus(path).getModificationTime()` is not defined.
It may be the time the file was initially created (in a `create()` call),
or the time data was last written to it.
It is *likely* to be in the time and time zone of the filesystem, rather
than that of the client.

### HDFS

That HDFS file metadata often lags the content of a file being written
to is not something everyone expects, nor convenient for any program trying
pick up updated data in a file being written. Most visible is the length
of a file returned in the various `list` commands and `getFileStatus` —this
is often out of data.

As HDFS only supports file growth in its output operations, this means
that the size of the file as listed in the metadata may be less than or equal
to the number of available bytes —but never larger. This is a guarantee which
is also held

One Algorithm to determine whether a file in HDFS is updated is:

1. Remember the last read position `pos` in the file, using `0` if this is the initial
read.
1. Use `getFileStatus(FS, Path)` to query the updated length of the file as
recorded in the metadata.
1. If `Status.length &gt pos`, the file has grown.
1. If the number has not changed, then
    1. reopen the file.
    1. `seek(pos)` to that location
    1. If `read() != -1`, there is new data.

This algorithm works for filesystems which are consistent with metadata and
data, as well as HDFS. What is important to know is that, in HDFS 
`getFileStatus(FS, path).getLen()==0` does not imply that `data(FS, path)` is
empty.

### Local Filesystem, `file:`

`LocalFileSystem`, `file:`, (or any other `FileSystem` implementation based on
`ChecksumFileSystem`) has a different issue. If an output stream
is obtained from `create()` and `FileSystem.setWriteChecksum(false)` has
*not* been called on the filesystem, then the FS only flushes as much
local data as can be written to full checksummed blocks of data.

That is, the flush operations are not guaranteed to write all the pending
data until the file is finally closed.

That is, `sync()`, `hsync()` and `hflush` may not persist all data written
to the stream.

For anyone thinking "this is a violation of this specification" —they are correct.
The local filesystem is intended for testing, rather than production use.

### Object Stores

Object store implementations historically cache the entire object's data
locally on disk (possibly in memory), until the final `close()` operation
triggers a single `PUT` of the data. Some implementations push out intermediate
blocks of data, synchronously or asynchronously.

Accordingly, they tend not to implement the `Syncable` interface.
However, Exception: Azure's `PageBlobOutputStream` does implement `hsync()`,
blocking until write operations being executed in separate threads have completed.

Equally importantly

1. The object may not be visible at the end of the path until the final `close()`.
is called; this holds for `getFileStatus()`, `open()` and all FileSystem list operations.
1. Any existing data at the end of a path `p`, may remain visible until the final
`close()` operation overwrites this data.
1. The check for existing data in a `create()` call with `overwrite==false`, may
take place in the `create()` call itself, in the `close()` call prior to/during
the write, or at some point in between. Expect in the special case that the
object store supports an atomic PUT operation, the check for existence of
existing data and the subsequent creation of data at the path contains a race
condition: other clients may create data at the path between the existence check
and the subsequent qrite.

