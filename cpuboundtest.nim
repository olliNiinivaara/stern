import workserver
import std/[typedthreads]

proc fib(n: int): int {.noinline.} =
  if isShuttingDown(): return
  if n <= 1:
    return n
  else:
    return fib(n - 1) + fib(n - 2)

proc fibonacci(http: Http): int =
  let response = "fib: " & $fib(20)
  http.add(response)

when isMainModule:
  let srv = startServer(8080, fibonacci)
  joinThread(srv.thread)
