## Cross-thread signaling using a pipe.
## Generic: works with any type T that is copyable by address.
## Safe to use from any thread;
## Linux-only (POSIX).

import posix, stagelane

proc eventfd*(initval: cuint, flags: cint): cint {.importc, header: "<sys/eventfd.h>".}

proc newEventFd*(): int =
  result = eventfd(0, O_CLOEXEC or O_NONBLOCK)

proc pokeEventFd*(fd: int) =
  var one: uint64 = 1
  discard write(cint(fd), addr one, sizeof(one))

proc drainEventFd*(fd: int): uint64 =
  var n: uint64 = 0
  discard read(cint(fd), addr n, sizeof(n))
  result = n

type
  SignalPipe*[T] = object
    readFd*: int
    writeFd*: int

proc newSignalPipe*[T](): SignalPipe[T] =
  result = SignalPipe[T]()
  var fds: array[0..1, cint]
  discard pipe(fds)
  result.readFd = int(fds[0])
  result.writeFd = int(fds[1])

proc send*[T](pipe: SignalPipe[T], value: T) =
  discard write(cint(pipe.writeFd), addr value, sizeof(T))

proc readValue*[T](pipe: SignalPipe[T]): T =
  var value: T
  discard read(cint(pipe.readFd), addr value, sizeof(T))
  return value

proc waitValue*[T](pipe: SignalPipe[T]) : T {.flow.} =
  waitReadable(pipe.readFd)
  result = readValue(pipe)

proc readMany*[T](pipe: SignalPipe[T], arr: var openArray[T]): int =
  let n = read(cint(pipe.readFd), addr arr[0], arr.len * sizeof(T))
  result = if n > 0: n div sizeof(T) else: 0

proc closeSignalPipe*[T](pipe: SignalPipe[T]) =
  discard close(cint(pipe.readFd))
  discard close(cint(pipe.writeFd))