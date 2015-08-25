# File Buffer

[![Clojars][clojars-img]][clojars-url]
[![Build Status][travis-image]][travis-url]
[![MIT License][license-image]][license]

## Overview

This package implements class FileBuffer that can be written to with a java.io.OutputStream and read via a java.io.InputStream. Writes go into the tail of the buffer and reads take place at the head. The implementation ensures that reading doesn't overtake writing which preserves the blocking semantics of java.io.InputStream.

```
                         +--------------+
                         |              |
  OutputStream.write ->  |  FileBuffer  | -> InputStream.read
                         |              |
                         +--------------+
```

FileBuffer was meant to allow concurrently downloading a large amount of data and reading the data -- without using too much memory. FileBuffer takes a threshold parameter that specificies the maximum # of bytes to buffer in memory before starting to buffer to a file. Furthermore, the overall in-memory portion of the buffer can be split into segments. When the reader of the FileBuffer reads beyond a segment, the memory for that segment can be freed.

```
  +--------------+     +--------------+          +------------+
  | MemorySegment| ->  | MemorySegment| -> .. -> | FileSegment|
  +--------------+     +--------------+          |            |
                                                 |            |
                                                 +------------+
```

The concurrency model of this code assumes a single reader and a single writer. However, the read and write can be on separate threads, of course.

## Usage

Todo: a basic example

## License

Copyright © 2015 Zensight, Inc.

Distributed under the MIT License. See [LICENSE][] for more details.

[license]: LICENSE
[license-image]: http://img.shields.io/badge/license-MIT-blue.svg?style=flat-square
[clojars-url]: https://clojars.org/co.zensight/file-buffer
[clojars-img]: https://img.shields.io/clojars/v/co.zensight/file-buffer.svg?style=flat-square
[travis-url]: http://travis-ci.org/Zensight/file-buffer
[travis-image]: http://img.shields.io/travis/Zensight/file-buffer.svg?style=flat-square
