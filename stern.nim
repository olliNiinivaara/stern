import posix, os 
import std/atomics
import std/[monotimes, times]

import stagelane, stagelane/[net]
import memorypool, threadpool, signaler 

const SHUTDOWN_CHECK_MS* {.intdefine.} = 500  ## drain progress check interval
const SHUTDOWN_STALL_MS* {.intdefine.} =  3000 ## hard-kill when inflightTasks hasn't decreased for this long
const MAX_SOCKET_TIMEOUT_MS* = 5000
# make these server-specific...

type
  HttpObj* = object
    server*: Server
    fd*: SocketHandle
    responsecode*: int
    requestbuffer*: int
    responsebuffer*: int
    headerEnd*: int
    methodStart*: int
    methodLen*: int
    pathStart*: int
    pathLen*: int

  Http* = ptr HttpObj

  MessageCallback* = proc(http: Http): int {.nimcall.}
  
  ServerObj* = object
    port*: int
    listener*: TcpListener
    thread*: Thread[Server]
    writeThread*: Thread[Server]
    callback*: MessageCallback
    signalpipe*: SignalPipe[HttpObj]
    freeSignalFd*: int
    freeWaitq*: WaitQueue
    waitingReaders*: Atomic[int]
    pool*: ptr MemoryPool
    workers*: ptr ThreadPool[Http]
    inflight*: Atomic[int]
    waiters*: seq[tuple[fd: int, deadline: int64]]
    tickerQ*: WaitQueue # refactor to something simpler...
    scope*: Scope

  Server* = ptr ServerObj

var servers*: seq[Server]
var shuttingdown*: Atomic[bool]

proc totalInflight*(): int =
  ## all servers' spawned worker threads not yet finished; the shutdown drain waits on this
  for s in servers: result += s.inflight.load(moRelaxed)

proc isShuttingDown*(): bool {.inline.} = shuttingdown.load(moRelaxed)

proc c_exit(status: cint) {.importc: "_exit", header: "<unistd.h>", noreturn.}

proc shutdown*() =
  {.gcsafe.}:
    if shuttingdown.load(moRelaxed):
      echo "\nExecuting hard shutdown!"
      c_exit(0)
    shuttingdown.store(true, moRelaxed)
    echo "\nExecuting graceful shutdown..."
    for s in servers:
      pokeEventFd(s.freeSignalFd)       # wake the reader thread's freeSignalLoop
      s.signalpipe.send(HttpObj())  # wake the writer thread's writeLoop
    var lastCount = totalInflight()
    var lastProgress = getMonoTime()
    while lastCount > 0:
      sleep(SHUTDOWN_CHECK_MS)
      let n = totalInflight()
      if n < lastCount:
        lastCount = n
        lastProgress = getMonoTime()
      elif getMonoTime() - lastProgress >= initDuration(milliseconds = SHUTDOWN_STALL_MS):
        echo "no task progress for ", SHUTDOWN_STALL_MS, "ms, ", n, " task(s) stuck — hard kill"
        c_exit(0)
    for s in servers: stopAndJoin(s.workers)
    c_exit(0)
 
{.hint[XDeclaredButNotUsed]:off.}
onSignal(SIGTERM): shutdown()
onSignal(SIGINT): shutdown()
{.hint[XDeclaredButNotUsed]:on.}

proc add*(http: Http, s: string) =
  http.server.pool.buffers[http.responsebuffer].putStr(s)
