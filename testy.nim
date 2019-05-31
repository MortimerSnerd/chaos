# Test harness for all libraries.
import 
  bplustree, strformat

proc go() = 
  echo "Testy"
  var tr = initBPlusTree[uint16, int32, float32](newInMemoryBlockMgr[uint16](128))

  for i in 1..300:
  #for i in countdown(300, 1):
    tr.add(int32(i), float32(i) * 7)

  tr.updateValue(int32(299), 34544.0f)

  assert int32(55) in tr

  for k, v in pairs(tr):
    echo &"k={k} v={v}"

  tr.withValue(int32(122), x):
    echo "HAH " & $x

  tr.withValue(int32(900), x):
    echo "NOPE " & $x

  graphviz(tr, "test.dot", false)
  echo "TREE = " & $tr

#
# var path: seq[int16]
# echo "FOUND " & repr(findLeafForKey(tr, int32(93), path))

go()
