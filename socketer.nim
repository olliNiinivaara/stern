import std/[os, posix, nativesockets, monotimes, strutils]
import stagelane, stagelane/[sched]
import memorypool, httpparsing, stern, signaler

const MAX_SOCKET_TIMEOUT_MS* = 5000
const INITIAL_BACKOFF_MS* = 1
const MAX_BACKOFF_MS* = MAX_SOCKET_TIMEOUT_MS

type
  ReadState = enum
    rsParsingMethod
    rsReadingHeaders
    rsReadingBody

  SocketError* = object of IOError

const
  MSG_DONTWAIT* = when defined(macosx): 0x80.cint else: 0x40.cint
  MSG_MORE* = 0x8000.cint
  intermediateflags: cint = MSG_NOSIGNAL + MSG_DONTWAIT + MSG_MORE
  lastflags: cint = MSG_NOSIGNAL + MSG_DONTWAIT

let
  version = "HTTP/1.1 "
  contentlen = "Content-Length: "
  shortdivider* = "\c\L"
  longdivider* = "\c\L\c\L"
  #[http200string = "200 OK\c\L"
  http200nocontent = "HTTP/1.1 200 OK\c\LContent-Length: 0\c\L\c\L"
  http200nocontentlen = 38
  http204string = "HTTP/1.1 204 No Content\c\L\c\L"
  http204stringlen = 27
  finishchunked = "0\c\L\c\L"
  zerocontent = "Content-Length: 0\c\L"]#

proc wouldBlock(): bool =
  let e = errno
  e == EAGAIN or e == EWOULDBLOCK or e == EINTR

proc sendToSocket(fd: SocketHandle, data: ptr UncheckedArray[char], len: int, flags: cint) {.flow.} =
  var off = 0
  while off < len:
    let r = posix.send(fd, unsafeAddr data[off], len - off, flags)
    if r >= 0: off += r
    elif wouldBlock():
      waitWritable(int(fd))
    else: raiseOSError(osLastError())

proc writeLoop*(server: Server) {.flow.} =
  var batch: array[256, HttpObj]
  var response = newStringOfCap(BUFFER_SIZE)
  while true:
    if isShuttingDown(): break
    waitReadable(server.signalpipe.readFd)
    let n = server.signalpipe.readMany(batch)  # (level-triggered...)
    for i in 0..<n:
      if isShuttingDown(): break
      let http = batch[i]
      try:
        response.setLen(0)
        response.add(version)
        response.add($http.responsecode)
        response.add(shortdivider)
        response.add(contentlen)
        response.add($server.pool.buffers[http.responsebuffer].n)
        response.add(longdivider)
        sendToSocket(http.fd, cast[ptr UncheckedArray[char]](addr(response[0])), response.len, intermediateflags)
        sendToSocket(http.fd, server.pool.buffers[http.responsebuffer].p, server.pool.buffers[http.responsebuffer].n, lastflags)
      except:
        if not isShuttingDown():  # during teardown the reader may have closed the fd already
          let msg = "socket send failure: " & $int(http.fd) & " : " & getCurrentExceptionMsg()
          if "Bad file descriptor" notin msg: echo msg 
        close(http.fd)
      finally:
        releaseResponseBuffer(server.pool, int(http.fd))


proc readRequest*(http: Http) {.flow.} =
  let pool = http.server.pool
  
  var state: ReadState = rsParsingMethod
  var headerEnd: int = -1
  var firstLineEnd: int = -1
  var contentLength: int = 0
  var remaining: int = 0
  var backoffMs = INITIAL_BACKOFF_MS
  var mStart, mLen, pStart, pLen = 0
  var currentLen = 0
  
  try:
    while true: 
      if currentLen >= pool.bufferSize:
        raise newException(SocketError,  "Buffer overflow: request too large")
      
      let toRead = case state
        of rsParsingMethod, rsReadingHeaders: pool.bufferSize - currentLen
        of rsReadingBody: remaining
      if toRead == 0: break

      if currentLen == 0:
        # Slowloris guard registration (reader thread only, no locks): this read is now
        # waiting for its first byte; the per-server sweeper reaps it if the deadline passes.
        # figure out some easier (yet fast) solution to time out stalled sockets...
        http.server.waiters.add((int(http.fd), getMonoTime().ticks div 1_000_000 + MAX_SOCKET_TIMEOUT_MS))
        notifyOne(http.server.tickerQ)
        waitReadable(int(http.fd))
      else:
        try:  
          withTimeout(backoffMs):
            waitReadable(int(http.fd))
          backoffMs = INITIAL_BACKOFF_MS
        except TimeoutError:
          if backoffMs >= MAX_BACKOFF_MS:
            raise newException(SocketError, "Socket timeout")
          backoffMs = min(backoffMs * 2, MAX_BACKOFF_MS)
          delay(backoffMs)
          continue

      let r = posix.recv(http.fd, unsafeAddr pool.buffers[http.requestbuffer].p[currentLen], toRead, 0)

      # refactor to stagehttp way?...: let st = parseRequest(c, w.cfg.maxHeaderBytes, w.cfg.maxBodyBytes, consumed)
      
      if r > 0:
        currentLen += r
        case state
        of rsParsingMethod:
          firstLineEnd = findFirstLineEnd(pool, http.requestbuffer, currentLen)
          if firstLineEnd == -1: continue
          let spans = httpparsing.parseRequestLine(pool, http.requestbuffer, firstLineEnd)
          mStart = spans.mStart; mLen = spans.mLen
          pStart = spans.pStart; pLen = spans.pLen
          if not hasRequestBody(pool.buffers[http.requestbuffer].p, mStart, mLen):
            state = rsReadingHeaders
            headerEnd = findHeaderEnd(pool, http.requestbuffer, currentLen - r, currentLen)
            if headerEnd >= 0:
              state = rsReadingBody
              contentLength = 0
              remaining = 0
              break
        of rsReadingHeaders:
          headerEnd = findHeaderEnd(pool, http.requestbuffer, currentLen - r, currentLen)
          if headerEnd == -1: continue
          if not hasRequestBody(pool.buffers[http.requestbuffer].p, mStart, mLen):
            contentLength = 0
            remaining = 0
            break
          else:
            contentLength = parseContentLength(pool, http.requestbuffer, headerEnd)
            if contentLength > pool.bufferSize - currentLen:
              raise newException(SocketError, "detected larger content-length header value than remaining buffer size")    
            if contentLength > 0:
              state = rsReadingBody
              let bodycount = currentLen - headerEnd
              remaining = contentLength - bodycount   
              if remaining <= 0: break
            else: break
        of rsReadingBody:
          remaining -= r
          if remaining <= 0: break
      elif r == 0:
        raise newException(SocketError, "Connection closed by peer")
      elif not wouldBlock():
        raiseOSError(osLastError())
      else:
        yieldNow()
        continue
    
    pool.buffers[http.requestbuffer].n = currentLen
    http.headerEnd = headerEnd
    http.methodStart = mStart
    http.methodLen = mLen
    http.pathStart = pStart
    http.pathLen = pLen
  except:
    raise  # buffer release is the caller's job (socketSession finally)
  finally:
    for i in 0 ..< http.server.waiters.len:
      if http.server.waiters[i].fd == int(http.fd):
        http.server.waiters.del(i)
        break