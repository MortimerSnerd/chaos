import ospaths, strutils

task build, "Builds test harness program":
    var outName : string

    when defined(windows):
      outName = "testy.exe"
    else:
      outName = "testy"

    setCommand "cpp", "testy"

    --deadCodeElim:on
    --listFullPaths
    --debuginfo
    --threads:on
    --threadAnalysis:on
    --define: debug

    --warnings:on
    --hints:on
    --colors:off
    --nanChecks:on
    --infChecks:on
    #--overflowChecks:on  # This is expensive for what we're doing.

    switch("path", ".")
    switch("out", outName)

when defined(windows):
  let DocIgnore : seq[string] = @[] 
else:
  let DocIgnore : seq[string] = @[]

let DocDirs = ["."]
let DocSearchDirs = ["."]

proc docPathOptions(inSubdir: bool) : string = 
  result = ""
  for d in DocDirs:
    if inSubDir:
      result &= " --path:" & "../" & d
    else:
      result &= " --path:" & d

proc filesToDoc() : seq[string] = 
  result = @[]
  for d in DocSearchDirs:
    for f in listFiles(d):
      echo "MAROG: " & f
      if not f.endswith(".nim") or f in DocIgnore:
        continue
        
      add(result, f)

task makeDocs, "Craps out html files left and right.":
  rmDir("html")
  mkDir("html")

  for f  in filesToDoc():
    let (dirName, fileName, ext) = splitFile(f)

    echo f & " --------------------------------------------------------"
    exec("nim doc2 " & docPathOptions(dirName != ".") & " -o:html/$1.html --index:on $3/$1$2" % [fileName, ext, dirName])

  exec("nim buildIndex -o:html/index.html html")
  setCommand "nop"

task makeJsonDocs, "Craps out html files left and right.":
  rmDir("json")
  mkDir("json")

  for f  in filesToDoc():
    let (dirName, fileName, ext) = splitFile(f)

    echo f & " --------------------------------------------------------"
    exec("nim jsondoc " & docPathOptions(dirName != ".") & " -o:json/$1.json --index:on $3/$1$2" % [fileName, ext, dirName])

  setCommand "nop"
