# Test harness for all libraries.
import 
  tbplustree, strformat

proc go() = 
  try:
    tbplustree.test()
  except:
    echo "EXCEPTION: " & getCurrentException().msg
    writeStackTrace()

#
# var path: seq[int16]
# echo "FOUND " & repr(findLeafForKey(tr, int32(93), path))

go()
