# ./wrk -t1 -c1000 -d5s --latency http://localhost:8080

import std/[posix, nativesockets, strutils, atomics, osproc, monotimes]
import stagelane, stagelane/[net]
import stern, memorypool, signaler, socketer, threadpool
export stern
  
proc processMessage(http: Http) {.nimcall, gcsafe.} =
  let server = http.server
  try:
    {.gcsafe.}:
      http.responsebuffer = acquireResponseBuffer(server.pool)
      http.responsecode = server.callback(http)
      if http.responsecode == 0: http.responsecode = 200
      server.signalpipe.send(http[])
  except:
    when not defined(release): raise
  finally:
    releaseRequestBuffer(server.pool, http.requestbuffer)
    server.inflight.atomicDec()
    if server.waitingReaders.load(moRelaxed) > 0: pokeEventFd(server.freeSignalFd)


proc freeSignalLoop(server: Server) {.flow.} =
  ## Runs on the reader thread. Worker threads poke freeSignalFd (plain eventfd write) after
  ## freeing their request memoery span; here we drain the counter and wake parked sessions
  ## waiting for for memory locally — no stagelane state crosses threads.
  while true:
    if isShuttingDown(): break
    waitReadable(server.freeSignalFd)
    discard drainEventFd(server.freeSignalFd)
    notifyAll(server.freeWaitq)
    if isShuttingDown():
      server.scope.cancel()
      # instead of canceling, better to close sockets?: posix.shutdown(fd.SocketHandle, SHUT_RDWR)
      break

proc socketSession(server: Server, s: TcpStream) {.flow.} =
  # When all request buffers are in flight, park on the reader-local queue until a worker frees one.
  var heldIdx = -1
  var http = HttpObj(server: server, fd: s.fd)
  try:
    while true:
      try:
        if isShuttingDown(): break
        while requestBuffersFree(server.pool) == 0:
          server.waitingReaders.atomicInc()
          let epoch = epochOf(server.freeWaitq)
          if requestBuffersFree(server.pool) > 0:
            server.waitingReaders.atomicDec()
            break
          parkSince(server.freeWaitq, epoch)
          server.waitingReaders.atomicDec()
        http.requestbuffer = acquireRequestBuffer(server.pool)
        heldIdx = http.requestbuffer
        readRequest(addr http)
        heldIdx = -1  # ownership passes to processMessage
        if isShuttingDown(): break
        server.inflight.atomicInc()
        server.workers.push(addr http)
      except IOError as e:
        if heldIdx >= 0: releaseRequestBuffer(server.pool, heldIdx); heldIdx = -1
        if e.msg == "No free request buffers available":
          continue  # lost the race for the last buffer; loop re-checks and parks
        raise
      except Exception as e:
        if heldIdx >= 0: releaseRequestBuffer(server.pool, heldIdx); heldIdx = -1
        if not e.msg.startsWith("Connection"): echo "[HANDLE MESSAGE] Error on fd=", int(s.fd), ": ", e.msg
        # echo e.getStackTrace()
        break
  finally:
    if heldIdx >= 0: releaseRequestBuffer(server.pool, heldIdx)
    s.close()

proc setTcpNoDelay(fd: SocketHandle) =
  var one = 1.cint
  discard setsockopt(fd, cint(posix.IPPROTO_TCP), posix.TCP_NODELAY, addr one, SockLen(sizeof(one)))

proc acceptLoop(server: Server) {.flow.} =
  server.listener = listenTcp(server.port, backlog = 4096)
  echo "newserver listening on port ", server.port, " fd=", int(server.listener.fd)
  while true:
    if isShuttingDown(): break
    var client: TcpStream
    try:
      client = server.listener.accept()
    except Exception as e:
      if isShuttingDown(): break
      echo "accept error on port ", server.port, ": ", e.msg
      # leads to infinite error loop?...
      continue
    if isShuttingDown(): break
    setTcpNoDelay(client.fd)
    discard go socketSession(server, client)

proc firstByteSweeper(server: Server) {.flow.} =
  ## Event-driven slowloris guard, all on the reader thread (no locks, no syscalls):
  ## silent (parked, no timer armed) while no read is waiting for its first byte;
  ## otherwise ONE shared timer aimed at the earliest waiter deadline.
  ## quite complex solution, check if timeout in rawreader would suffice...
  while true:
    if isShuttingDown(): break
    if server.waiters.len == 0:
      park(server.tickerQ)            # silent: nothing waiting, no timer
      continue
    let now = getMonoTime().ticks div 1_000_000
    var minDl = high(int64)
    for w in server.waiters: minDl = min(minDl, w.deadline)
    let flag = startTimeout(max(1, minDl - now).int, server.tickerQ)
    park(server.tickerQ)              # wakes on the timer OR on a new waiter's notifyOne
    cancel(flag.timer)
    let now2 = getMonoTime().ticks div 1_000_000
    var i = 0
    while i < server.waiters.len:
      if server.waiters[i].deadline <= now2:
        discard posix.shutdown(SocketHandle(server.waiters[i].fd), SHUT_RDWR)  # wakes the parked read with HUP
        server.waiters.del(i)
      else:
        inc i

proc readThread*(server: Server) {.flow.} = 
  let listeningloop = server.scope.go acceptLoop(server)
  discard server.scope.go freeSignalLoop(server)
  discard server.scope.go firstByteSweeper(server)
  await listeningloop
 
proc writeThread*(server: Server) {.flow.} =
  await server.scope.go writeLoop(server)
  
proc runWriteThread(server: Server) {.thread, nimcall.} =
  try:
    {.gcsafe.}: run writeThread(server)
  except CancelledError: echo "write thread stopped for server at port ", server.port
  except Exception as e: echo "write thread error (port ", server.port, "): ", e.msg

proc runReadThread(server: Server) {.thread, nimcall.} =
  try:
    {.gcsafe.}: run readThread(server)
  except CancelledError: echo "read thread stopped for server at port ", server.port
  except Exception as e: echo "read thread error (port ", server.port, "): ", e.msg


proc startServer*(port: int, callback: MessageCallback,
                  requestBuffers = DEFAULT_REQUEST_BUFFERS,
                  responseBuffers = DEFAULT_RESPONSE_BUFFERS,
                  workerCount = countProcessors()): Server =
  # Usage:
  #   let srv  = startServer(8080, myCallback)
  #   let adm  = startServer(8081, adminCallback, requestBuffers = 256, responseBuffers = 256, , workerCount = 2)
  #   joinThreads(srv.thread, adm.thread)
  echo "server on port ", port, ": request pool ", requestBuffers, " + response pool ",
       responseBuffers, " x ", BUFFER_SIZE, " B, worker pool ", workerCount, " threads"
  signal(SIGPIPE, SIG_IGN)  # sends use MSG_NOSIGNAL, never die on SIGPIPE
  result = createShared(ServerObj)
  result.port = port
  result.pool = newBufferPool(requestBuffers, responseBuffers)
  result.workers = newPool[Http](workerCount, requestBuffers + 16, processMessage)  # ringCap >= max in-flight
  result.signalpipe = newSignalPipe[HttpObj]()
  result.freeSignalFd = newEventFd()
  result.freeWaitq = newWaitQueue()
  result.waitingReaders.store(0, moRelaxed)
  result.inflight.store(0, moRelaxed)
  result.tickerQ = newWaitQueue()
  result.callback = callback
  result.scope = newScope()
  servers.add(result)
  createThread(result.writeThread, runWriteThread, result)
  createThread(result.thread, runReadThread, result)