# Test harness for all libraries.
import 
  os, random, tbplustree, times, strformat, strutils

proc go() = 
  try:
    if paramCount() > 0:
      let seed = parseBiggestInt(paramStr(1))
      echo "SEED set to " & $seed
      randomize(int64(seed))
      tbplustree.test()
    else:
      for i in 1..100:
        tbplustree.test()
        let now = times.getTime()
        let seed = convert(Seconds, Nanoseconds, now.toUnix) + now.nanosecond

        echo ""
        echo "SEED=" & $seed
        randomize(seed)

  except:
    echo "EXCEPTION: " & getCurrentException().msg
    writeStackTrace()

#
# var path: seq[int16]
# echo "FOUND " & repr(findLeafForKey(tr, int32(93), path))

go()
