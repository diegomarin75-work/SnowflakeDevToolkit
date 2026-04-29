#Import libraries
import io
import os
import re
import sys
import json
import time
import shutil
import fnmatch
import hashlib
import chardet
import subprocess
import tempfile
import datetime
import collections
import jinja2
import pathlib
import contextlib
import traceback
import threading
from printlib import PrintingLibrary

#Constants
EXEC_SCRIPT_EXT=".sql"
CFG_ENV_VAR_NAME="SF_CONFIG_PATH"
CFG_FILE_NAME="sf-cfg.json"
HIS_FILE_NAME="sf-his.json"
CSV_SEPARATOR=","
SF_CON_VARIABLE="SNOWFLAKE_CONN"
PRELOAD_LIBRARIES_TIMEOUT_SECS=30

#Run mode set options
RUN_MODE_OPTIONS=["--exec-file","--exec-folder","--exec-changes","--exec-diff","--test-file","--test-folder","--test-changes","--test-diff",
                  "--schema-list","--schema-drop","--repl-full","--repl-changes","--repl-diff","--repl-file","--sql","--macro-list","--macro-detail","--<macro>"]

#Snowflake type codes
SNOWFLAKE_TYPE_CODES={0 :"int", 1 :"real", 2 :"string", 3 :"date", 4 :"timestamp", 5 :"variant", 6 :"timestamp_ltz", 7 :"timestamp_tz", 
                      8 :"timestamp_tz", 9 :"object", 10:"array", 11:"binary", 12:"time", 13:"boolean", 14:"geography", 15:"geometry", 16:"vector"  }

#Global variablees
_ImportSnowflakeError=None
_SnowflakeLibrariesReady=threading.Event() #Event that signals when Snowflake libraries are loaded

#----------------------------------------------------------------------------------------------------------------------
# Show help
#----------------------------------------------------------------------------------------------------------------------
def ShowHelp():
  print("Snowflake development toolkit - v1.0 - Diego Marin 2025")
  print("")
  print("Usage:")
  print("Script execution > python sf.py (--exec-file:<name>|--exec-folder:<name>|--exec-changes|--exec-diff:<branch>) --con:<name> [--sfcon:<file>] [--force] [--ignore-hash] [--show] [--debug] [--silent]")
  print("Script test      > python sf.py (--test-file:<name>|--test-folder:<name>|--test-changes|--test-diff:<branch>) --con:<name> [--sfcon:<file>] [--force] [--ignore-hash] [--show] [--debug] [--silent]")
  print("Schema operation > python sf.py (--schema-list:<schema>|--schema-drop:<schema>) --con:<name> [--sfcon:<file>] [--silent]")
  print("Code replication > python sf.py (--repl-full|--repl-changes|--repl-diff:<branch>|--repl-file:<pattern>) [--lanes:<sour>,<dest>] [--update] [--sfcon:<file>]")   
  print("SQL query        > python sf.py (--sql:<text>) --con:<name> [--sfcon:<file>] [--payload] [--types] [--sep] [--csv] [--show] [--debug] [--silent]")
  print("Macros           > python sf.py (--<macro>(<arg>)|--macro-list|--macro-detail:<filter>)")
  print("")
  print("Run modes:")
  print("--exec-file:<name>       : Execute single sql script file")
  print("--exec-folder:<name>     : Execute all sql scripts inside folder (and subfolders)")
  print("--exec-changes           : Execute all detected changed/added sql scripts in git repo")
  print("--exec-diff:<branch>     : Execute all detected different sql scripts in git repo from diff comparison to <branch>")
  print("--test-file:<name>       : Execute in test mode single sql script file")
  print("--test-folder:<name>     : Execute in test mode all sql scripts inside folder (and subfolders)")
  print("--test-changes           : Execute in test mode all detected changed/added sql scripts in git repo from diff comparison to <branch>")
  print("--test-diff:<branch>     : Execute in test mode all different sql scripts in git repo")
  print("--schema-list:<schema>   : List al objects in a given schema (schema name using current connection or fully qualified)")
  print("--schema-drop:<schema>   : Drops all objects in a given schema but not the schema (schema name using current connection or fully qualified)")
  print("--repl-full              : Full replication mode (wipes destination lane files before copy)")
  print("--repl-changes           : Replicate only all modified files detected by git")
  print("--repl-diff:<branch>     : Replicate only all different files detected by git from diff comparison to <branch>")
  print("--repl-file:<pattern>    : Replicate only specified files (pattern accepts wildcards * and ?)")
  print("--sql:<text>             : SQL statement to execute (statement can contain SQL macros)")
  print("--<macro>(<arg>)         : Run macro (either python or SQL), arguments separated by colon (:)")
  print("--macro-list             : Shows help about defined macros")
  print("--macro-detail:<pattern> : Shows full detail about a defined macro (pattern accepts wildcards * and ?)")
  print("")
  print("Options:")
  print("--sfcon:<name>         : Give path of snowflake connections.toml file")
  print("--con:<name>           : Connection name(s) to use (acccording to snowflake connections.toml file)")
  print("--lanes:<src>,<dst>    : Replicate code from <src> lane into <dst> lane")
  print("--update               : DestinUpdate flag, enable actual modification of files in project replication")
  print("--force                : Do not ask user before executing scripts")
  print("--ignore-hash          : Ignore file hash when checking if a script has been executed already")
  print("--ignore-schema-check  : Ignores forbidden schema check when replicating files between source code lanes")
  print("--payload              : Executes query as payload inside another query (only for --sql option)")
  print("--types                : Display output column metadata instead of query results (only for --sql option)")
  print("--sep                  : Do not combine outputs from several connections, display separated (only for --sql option)")
  print("--csv                  : Output results in CSV format (only for --sql option). Enables also --silent flag.")
  print("--show                 : Display only SQL statements that would be executed without executing anything")
  print("--debug                : Display every SQL query before executing it")
  print("--silent               : Do not print any messages, only errors")
  print("")
  print("Notes:")
  print("- Sql scripts changed/added on git repo are those returned by \"git status --porcelain=v1 --untracked-files=all\" command.")
  print("- Scripts are executed only if modification date is earlier than last execution date")
  print("- Test mode for execution of scripts is implemented by replicating scripts into a test database schema with full permissions")
  print("- Only for --sql mode, several connection names can be given (separated by comma)")
  print("- Snowflake connections.toml file is read from environment variable SNOWFLAKE_HOME if present, if not it must be passed using --sfcon option")
  print("- For code replication, top level of current git repository must be configured in configuration file")
  
#----------------------------------------------------------------------------------------------------------------------
# Get command line arguments
#----------------------------------------------------------------------------------------------------------------------
def GetCommandLineOptions():

  #Default values for options
  ExecFileName=""
  ExecFolderName=""
  ExecChanges=False
  ExecDiffBranch=""
  TestFileName=""
  TestFolderName=""
  TestChanges=False
  TestDiffBranch=""
  SqlQuery=""
  ConnectionName=""
  ConnectionsFile=(os.environ[SF_CON_VARIABLE] if SF_CON_VARIABLE in os.environ else "")
  ListSchema=""
  DropSchema=""
  CsvOutput=False
  SilentMode=False
  ForceMode=False
  IgnoreHash=False
  IgnoreSchemaCheck=False
  PayloadMode=False
  DisplayTypes=False
  CombineResults=True
  ShowMode=False
  DebugMode=False
  MacroSpec=""
  MacroList=False
  MacroDetail=False
  ReplFull=False
  ReplChanges=False
  ReplDiffBranch=""
  ReplFile=False
  FilePattern=""
  SourceLane=""
  DestinLane=""
  DestinUpdate=True
  MacroFilter="*"
  
  #Get arguments
  for i in range(1,len(sys.argv)):
    Item=sys.argv[i]
    if Item.startswith("--exec-file:"):
      ExecFileName=Item.replace("--exec-file:","")
    elif Item.startswith("--exec-folder:"):
      ExecFolderName=Item.replace("--exec-folder:","")
    elif Item=="--exec-changes":
      ExecChanges=True
    elif Item.startswith("--exec-diff:"):
      ExecDiffBranch=Item.replace("--exec-diff:","")
    elif Item.startswith("--test-file:"):
      TestFileName=Item.replace("--test-file:","")
    elif Item.startswith("--test-folder:"):
      TestFolderName=Item.replace("--test-folder:","")
    elif Item=="--test-changes":
      TestChanges=True
    elif Item.startswith("--test-diff:"):
      TestDiffBranch=Item.replace("--test-diff:","")
    elif Item.startswith("--sql:"):
      SqlQuery=Item.replace("--sql:","")
    elif Item.startswith("--schema-list:"):
      ListSchema=Item.replace("--schema-list:","")
    elif Item.startswith("--schema-drop:"):
      DropSchema=Item.replace("--schema-drop:","")
    elif Item=="--macro-list":
      MacroList=True
    elif Item.startswith("--macro-detail:"):
      MacroDetail=True
      MacroFilter=Item.replace("--macro-detail:","")
    elif Item.startswith("--con:"):
      ConnectionName=Item.replace("--con:","")
    elif Item.startswith("--sfcon:"):
      ConnectionsFile=Item.replace("--sfcon:","")
    elif Item=="--silent":
      SilentMode=True
    elif Item=="--csv":
      CsvOutput=True
      SilentMode=True
    elif Item=="--force":
      ForceMode=True
    elif Item=="--ignore-hash":
      IgnoreHash=True
    elif Item=="--ignore-schema-check":
      IgnoreSchemaCheck=True
    elif Item=="--payload":
      PayloadMode=True
    elif Item=="--types":
      DisplayTypes=True
    elif Item=="--sep":
      CombineResults=False
    elif Item=="--show":
      ShowMode=True
    elif Item=="--debug":
      DebugMode=True
    elif Item=="--repl-full":
      ReplFull=True
    elif Item=="--repl-changes":
      ReplChanges=True
    elif Item.startswith("--repl-diff:"):
      ReplDiffBranch=Item.replace("--repl-diff:","")
    elif Item.startswith("--repl-file:"):
      ReplFile=True
      FilePattern=Item.replace("--repl-file:","")
    elif Item.startswith("--lanes:"):
      SourceLane=Item.replace("--lanes:","").split(",")[0]
      DestinLane=Item.replace("--lanes:","").split(",")[1]
    elif Item=="--update":
      DestinUpdate=True
    elif re.search(r"^--([a-z0-9_]+)\(",Item)!=None:
      MacroSpec=Item[2:]
    else:
      print("Invalid option: ",Item)
      return False,{}
  
  #Detect run mode
  RunModes={
    "EXEC-FILE":len(ExecFileName)!=0,"EXEC-FOLDER":len(ExecFolderName)!=0,"EXEC-CHANGES":ExecChanges==True,"EXEC-DIFF":len(ExecDiffBranch)!=0,
    "TEST-FILE":len(TestFileName)!=0,"TEST-FOLDER":len(TestFolderName)!=0,"TEST-CHANGES":TestChanges==True,"TEST-DIFF":len(TestDiffBranch)!=0,
    "SCHEMA-LIST":len(ListSchema)!=0,"SCHEMA-CLEAN":len(DropSchema)!=0,"EXEC-SQL":len(SqlQuery)!=0,
    "REPL-FULL":ReplFull==True,"REPL-CHANGES":ReplChanges==True,"REPL-DIFF":len(ReplDiffBranch)!=0,"REPL-FILE":ReplFile==True,
    "MACRO-RUN":len(MacroSpec)!=0,"MACRO-LIST":MacroList==True,"MACRO-DETAIL":MacroDetail==True
  }
  ActiveModes=[Mode for Mode,Value in RunModes.items() if Value==True]
  if len(ActiveModes)==1:
    RunMode=ActiveModes[0]
  else:
    print("Must specify exactly only one of these options: "+", ".join(RUN_MODE_OPTIONS))
    return False,{}

  #Must specify --sfcon if environment variable is not set
  if RunMode in ["EXEC-FILE","EXEC-FOLDER","EXEC-CHANGES","EXEC-DIFF","TEST-FILE","TEST-FOLDER","TEST-CHANGES","TEST-DIFF","EXEC-SQL","SCHEMA-LIST","SCHEMA-CLEAN"] and len(ConnectionsFile)==0:
    print(f"Environment variable {SF_CON_VARIABLE} not set, must provide path of snowflake connections.toml file using --sfcon option")
    return False,{}
  
  #Check options for multiple connection modes
  if RunMode in ["EXEC-FILE","EXEC-FOLDER","EXEC-CHANGES","EXEC-DIFF","TEST-FILE","TEST-FOLDER","TEST-CHANGES","TEST-DIFF","SCHEMA-LIST","SCHEMA-CLEAN"]:
    if len(ConnectionName)==0:
      print("Must provide connection name (--con option)")
      return False,{}
    if len(ConnectionName.split(","))>1:
      print("Multiple connection mode is only for --sql mode")
      return False,{}
    if PayloadMode==True:
      print("Provide --payload parameter only with --sql option")
      return False,{}
    if DisplayTypes==True:
      print("Provide --types parameter only with --sql option")
      return False,{}
    if CombineResults==False:
      print("Provide --sep parameter only with --sql option")
      return False,{}
    if CsvOutput==True:
      print("Provide --csv parameter only with --sql option")
      return False,{}

  #Check connections for single connection modes
  if RunMode in ["EXEC-SQL"]:
    if len(ConnectionName)==0:
      print("Must provide connection name (--con option)")
      return False,{}
    if ForceMode==True:
      print("Do not provice --force parameter with --sql, does not have any effect")
      return False,{}
    if IgnoreHash==True:
      print("Do not provice --ignore-hash parameter with --sql, does not have any effect")
      return False,{}

  #Check source code lanes in code replication modes
  if RunMode in ["REPL-FULL","REPL-CHANGES","REPL-DIFF","REPL-FILE"]:
    if len(SourceLane)==0 or len(DestinLane)==0:
      print("Provide source and destination source code lanes for project replication modes!")
      return False,{}
  
  #Check macro filter is given in macro detail mode
  if RunMode in ["MACRO-DETAIL"]:
    if len(MacroFilter)==0:
      print("Provide macro name filter value!")
      return False,{}
    
  #Schema check must only be provided in replication of testing modes
  if RunMode not in ["TEST-FILE","TEST-FOLDER","TEST-CHANGES","TEST-DIFF","REPL-FULL","REPL-CHANGES","REPL-DIFF","REPL-FILE"] and IgnoreSchemaCheck==True:
    if IgnoreSchemaCheck==True:
      print("Do not provice --ignore-schema-check parameter with non code testing or replication modes, does not have any effect")
      return False,{}

  #Return arguments
  Options={}
  Options["run_mode"]=RunMode
  Options["exec_file_name"]=ExecFileName
  Options["exec_folder_name"]=ExecFolderName
  Options["exec_diff_branch"]=ExecDiffBranch
  Options["test_file_name"]=TestFileName
  Options["test_folder_name"]=TestFolderName
  Options["test_diff_branch"]=TestDiffBranch
  Options["list_schema"]=ListSchema
  Options["drop_schema"]=DropSchema
  Options["sql_query"]=SqlQuery
  Options["connection_name"]=ConnectionName
  Options["connections_file"]=ConnectionsFile
  Options["csv_output"]=CsvOutput
  Options["silent_mode"]=SilentMode
  Options["force_mode"]=ForceMode
  Options["ignore_hash"]=IgnoreHash
  Options["ignore_schema_check"]=IgnoreSchemaCheck
  Options["payload_mode"]=PayloadMode
  Options["display_types"]=DisplayTypes
  Options["combine_results"]=CombineResults
  Options["show_mode"]=ShowMode
  Options["debug_mode"]=DebugMode
  Options["repl_diff_branch"]=ReplDiffBranch
  Options["file_pattern"]=FilePattern
  Options["source_lane"]=SourceLane
  Options["destin_lane"]=DestinLane
  Options["destin_update"]=DestinUpdate
  Options["macro_spec"]=MacroSpec
  Options["macro_filter"]=MacroFilter

  #Return code
  return True,Options

# ----------------------------------------------------------------------------------
# Loads JSON configuration file
# (the files can contain comments (started by //), since they are filtered before 
# ----------------------------------------------------------------------------------  
def JsonFileParser(FilePath):

  #Replaces new lines inside strings as \n (as standard JSON requires)  
  def FixMultilineJson(Content):
    Output=[]
    StringMode=False
    EscapeMode=False
    for Chr in Content:
      if StringMode:
        if EscapeMode:
          Output.append(Chr)
          EscapeMode=False
        elif Chr=="\\":
          Output.append(Chr)
          EscapeMode=True
        elif Chr=='"':
          Output.append(Chr)
          StringMode=False
        elif Chr=="\n":
          Output.append("\\n")
        else:
          Output.append(Chr)
      else:
        if Chr=='"':
          Output.append(Chr)
          StringMode=True
        else:
          Output.append(Chr)
    return "".join(Output)
  
  #Import data sources file
  try:
    File=open(FilePath,"r")
    FileContent=File.read()
    File.close()
    FileContent="\n".join([Line for Line in FileContent.split("\n") if Line.strip().startswith("//")==False])
    FileContent=FixMultilineJson(FileContent)
    Json=json.loads(FileContent)
  except Exception as Ex:
    Message=f"Exception reading configuration file ({FilePath}): {str(Ex)}"
    return False,Message,None

  #Return result
  return True,"",Json

# ----------------------------------------------------------------------------------
# Set absolute path from a given path
# ----------------------------------------------------------------------------------
def AbsPath(FilePath):
  AbsFilePath=os.path.normpath(os.path.abspath(FilePath))
  if len(AbsFilePath)>=2 and AbsFilePath[0].lower()>="a" and AbsFilePath[0].lower()<="z" and AbsFilePath[1]==":":
    AbsFilePath=AbsFilePath[0].lower()+AbsFilePath[1:]
  return AbsFilePath

# ----------------------------------------------------------------------------------
# Execute command
# ----------------------------------------------------------------------------------
def Exec(Command):
  Proc=subprocess.Popen(Command,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
  StdOut,_=Proc.communicate()
  RetCode=Proc.returncode
  Lines=(StdOut.decode(errors="ignore") if StdOut else "")
  Output=""
  for Line in Lines:
    Output+=str(Line).replace("b'","").replace("'","").replace("\\n","\n").replace("\\r","")
  return RetCode,Output

# ---------------------------------------------------------------------------------------------------------------------
# Detect top level of git repo
# ---------------------------------------------------------------------------------------------------------------------
def GetGitRepoTopLevel(Path="."):
  RetCode,Output=Exec(f"git -C {Path} rev-parse --show-toplevel")
  if RetCode==0:
    return AbsPath(Output.replace("\n",""))
  else:
    return None

# ---------------------------------------------------------------------------------------------------------------------
# Check connection is ready
# ---------------------------------------------------------------------------------------------------------------------
def CheckConnectionReadiness(Config,WaitReady=False):
  
  #Get connection check parameters
  IntranetWlans=Config["intranet_wlans"]
  RemoteWlans=Config["remote_wlans"]
  ConnectedWlanCommand=Config["connected_wlan_command"]
  NetworkInterfacesCommand=Config["network_interfaces_command"]
  VpnInterfaceName=Config["vpn_interface_name"]
  RetrySleepSeconds=Config["retry_sleep_seconds"]
  RetryTimes=Config["retry_times"]  

  #Wait loop
  RetryNr=0
  while RetryNr<RetryTimes:
  
    #Get connected WLANs
    RetCode,Output=Exec(ConnectedWlanCommand)
    if RetCode!=0:
      _pr.Print(f"Unable to check current connected WLAN. Command \"{ConnectedWlanCommand}\" returned status {RetCode}")
      _pr.Print(Output)
      return False
    
    #Detect if we are already on intranet network
    for Wlan in IntranetWlans:
      if Wlan in Output:
        return True
      
    #Detect if we are on remote network
    RemoteNetwork=False
    for Wlan in RemoteWlans:
      if Wlan in Output:
        RemoteNetwork=True
        break
    if RemoteNetwork==False and WaitReady==False:
      _pr.Print("Connected to unknown network!")
      return False
    
    #Detect if VPN is connected
    RetCode,Output=Exec(NetworkInterfacesCommand)
    if RetCode!=0:
      _pr.Print(f"Unable to check VPN status. Command \"n{NetworkInterfacesCommand}\" returned status {RetCode}")
      _pr.Print(Output)
      return False
    if VpnInterfaceName in Output:
      return True
    elif WaitReady==False:
      _pr.Print(f"Network not ready, VPN interface is not connected!")
      return False
    
    #Try again
    time.sleep(RetrySleepSeconds)
    RetryNr+=1

  #Return connection not ready
  return False

# ---------------------------------------------------------------------------------------
# Format time
# ---------------------------------------------------------------------------------------
def FormatSeconds(TotalSecs):
  Hours=int(TotalSecs//3600)
  Minutes=int((TotalSecs%3600)//60)
  Secs=TotalSecs%60
  TimeParts=[]
  if Hours>0:
    TimeParts.append(f"{Hours}h")
  if Minutes>0 or Hours>0:
    TimeParts.append(f"{Minutes}m")
  TimeParts.append(f"{Secs:.2f}s")
  return " ".join(TimeParts)

#----------------------------------------------------------------------------------------------------------------------
# _pr.Print CSV
#----------------------------------------------------------------------------------------------------------------------
def PrintCsv(Heading,Rows,Separator):
  print(Separator.join(Heading))
  for Row in Rows:
    print(Separator.join([(str(Value).replace("\n"," ").replace("\r","") if str(Value).find(Separator)==-1 else "\""+str(Value).replace("\"","\"\"").replace("\n"," ").replace("\r","")+"\"") for Value in Row]))
    
#----------------------------------------------------------------------------------------------------------------------
# _pr.Print results
#----------------------------------------------------------------------------------------------------------------------
def PrintResults(Heading1,Heading2,ColAttributes,Rows,CsvOutput):
  if CsvOutput==False:
    _pr.PrintTable(Heading1,Heading2,ColAttributes,Rows)
  else:
    PrintCsv(Heading1,Rows,CSV_SEPARATOR)
    
# ---------------------------------------------------------------------------------------------------------------------
# _pr.Print code
# ---------------------------------------------------------------------------------------------------------------------
def CodePrint(Code):
  MaxLines=len(Code.split("\n"))+1
  MaxLength=len(str(MaxLines))
  for Index,Line in enumerate(Code.split("\n")):
    _pr.Print(str(Index+1).rjust(MaxLength)+" : "+Line)

# ---------------------------------------------------------------------------------------------------------------------
# Daemon to load snowflake libraries
# ---------------------------------------------------------------------------------------------------------------------
def ImportSnowflakeLibrariesDaemon():
  global _ImportSnowflakeError
  global _SnowflakeLibrariesReady
  _ImportSnowflakeError=None
  try:
    from snowflake.connector import connect
    from snowflake.connector.util_text import split_statements
    globals()["SnowflakeConnect"]=connect
    globals()["SnowflakeSplitStatements"]=split_statements
  except Exception as Ex:
    _ImportSnowflakeError="Exception happened while loading snowflake libraries: "+str(Ex)
  finally:
    _SnowflakeLibrariesReady.set()

# ---------------------------------------------------------------------------------------------------------------------
# Preload snowflake libraries
# ---------------------------------------------------------------------------------------------------------------------
def PreloadSnowflakeLibraries():
  threading.Thread(target=ImportSnowflakeLibrariesDaemon,daemon=True).start()

# ---------------------------------------------------------------------------------------------------------------------
# Import snowflake libraries
# (wait for daemon to finish)
# ---------------------------------------------------------------------------------------------------------------------
def ImportSnowflakeLibraries():
  _SnowflakeLibrariesReady.wait(PRELOAD_LIBRARIES_TIMEOUT_SECS)
  if not _SnowflakeLibrariesReady.is_set():
    Message="Snowflake libraries load timeout reached!"
    return False,Message
  if _ImportSnowflakeError!=None:
    return False,_ImportSnowflakeError
  else:
    return True,""

# ---------------------------------------------------------------------------------------------------------------------
# Detect binary file
# ---------------------------------------------------------------------------------------------------------------------
def DetectBinaryFile(FilePath,NumBytes=10000):
  File=open(FilePath,"rb")
  RawData=File.read(NumBytes)
  File.close()
  TextChars=bytearray([8,9,10,12,13]+[i for i in range(32,126)])
  NonTextData=RawData.translate(None,delete=TextChars)
  Ratio=(len(NonTextData)/len(RawData) if len(RawData)!=0 else 0)
  if Ratio>0.3:
    BinaryFile=True
  else:
    BinaryFile=False
  return BinaryFile

# ---------------------------------------------------------------------------------------------------------------------
# Detect file encoding
# ---------------------------------------------------------------------------------------------------------------------
def DetectFileEncoding(FilePath,NumBytes=100000):
  File=open(FilePath,"rb")
  RawData=File.read(NumBytes)
  File.close()
  Result=chardet.detect(RawData)
  return Result["encoding"]

# ---------------------------------------------------------------------------------------------------------------------
# Count files in path
# ---------------------------------------------------------------------------------------------------------------------
def CountFiles(RootPath,Folders):
  TotalFiles=0
  for Folder in Folders:
    Path=os.path.join(RootPath,Folder)
    for _,_,Files in os.walk(Path):
      TotalFiles+=len(Files)
  return TotalFiles

# ---------------------------------------------------------------------------------------------------------------------
# Get last file execution date on the connection name
# ---------------------------------------------------------------------------------------------------------------------
def GetFileExecutionDateHash(FilePath,ConnectionName):
  HisFile=os.path.join(tempfile.gettempdir(),HIS_FILE_NAME)
  if not os.path.exists(HisFile):
    return True,"",None,None
  try:
    File=open(HisFile,"r",encoding="utf-8")
    Log=json.load(File)
    File.close()
  except Exception as Ex:
    Message=f"Unable to read log file: {str(Ex)}"
    return False,Message,None,None
  Key=f"{AbsPath(FilePath)}:{ConnectionName}"
  if Key in Log:
    TimeStamp=datetime.datetime.fromisoformat(Log[Key]["modif_datetime"])
    FileHash=Log[Key]["file_hash"]
  else:
    TimeStamp=None
    FileHash=None
  return True,"",TimeStamp,FileHash

# ---------------------------------------------------------------------------------------------------------------------
# DestinUpdate file execution date on the connection name
# ---------------------------------------------------------------------------------------------------------------------
def UpdateFileExecutionDateHash(FilePath,ConnectionName):
  HisFile=os.path.join(tempfile.gettempdir(),HIS_FILE_NAME)
  Json={}
  if os.path.exists(HisFile):
    try:
      File=open(HisFile,"r",encoding="utf-8")
      Json=json.load(File)
      File.close()
    except (json.JSONDecodeError, IOError):
      Json={}
  File=open(FilePath,"rb")
  FileHash=hashlib.sha256(File.read()).hexdigest()
  File.close()
  Key=f"{AbsPath(FilePath)}:{ConnectionName}"
  Json[Key]={"modif_datetime":datetime.datetime.now().isoformat(),"file_hash":FileHash}
  File=open(HisFile,"w",encoding="utf-8")
  json.dump(Json,File,indent=2)
  File.close()

# ---------------------------------------------------------------------------------------------------------------------
# Detect if a file has been already execyted
# ---------------------------------------------------------------------------------------------------------------------
def GetAlreadyExecuted(FilePath,ConnectionName,IgnoreHash):
  
  #Get execution date of file
  Status,Message,ExecDateTime,LastFileHash=GetFileExecutionDateHash(FilePath,ConnectionName)
  if Status==False:
    return False,Message,None,""
  
  #Get modification date of file
  ModTimeStamp=os.path.getmtime(FilePath)
  ModifDateTime=datetime.datetime.fromtimestamp(ModTimeStamp)

  #Get file hash
  if IgnoreHash==False:
    File=open(FilePath,"rb")
    CurrentFileHash=hashlib.sha256(File.read()).hexdigest()
    File.close()
  else:
    CurrentFileHash=None

  #Return result
  if (ExecDateTime!=None and ExecDateTime>ModifDateTime) or (LastFileHash!=None and LastFileHash==CurrentFileHash):
    return True,"",True,(ExecDateTime.isoformat() if ExecDateTime!=None else None)
  else:
    return True,"",False,""

# ---------------------------------------------------------------------------------------------------------------------
# Strip SQL comments inside a SQL statement
# ---------------------------------------------------------------------------------------------------------------------
def StripSqlComments(Sql):
  Output=[]
  InsideString=False
  Index=0
  while Index<len(Sql):
    if not InsideString and Sql[Index:Index+2]=='--':
      Index=Sql.find('\n',Index)
      if Index==-1:
        break
    elif not InsideString and Sql[Index:Index+2]=='/*':
      Index=Sql.find('*/',Index+2)
      if Index==-1:
        break
      Index+=2
    else:
      if (Sql[Index]=="'" and not (Index>0 and Sql[Index-1]=="\\")) or Sql[Index:Index+2]=='$$':
        InsideString=(True if InsideString==False else False)
      Output.append(Sql[Index])
      Index+=1
  return ''.join(Output)

# ---------------------------------------------------------------------------------------------------------------------
# Get query execution mode
# ---------------------------------------------------------------------------------------------------------------------
def GetQueryExecutionMode(Query,ConnectionName,Config):
  Sql=StripSqlComments(Query.replace("\t"," ").strip()).replace("\n"," ").strip().upper()
  FoundExecMode=None
  for ExecutionRule in Config["connections"][ConnectionName]["execution_rules"]:
    for Rule in Config["execution_rules_def"][ExecutionRule]:
      Regex=Rule["regex"]
      ExecMode=Rule["exec_mode"]
      RegexMatch=(False if re.match(Regex,Sql)==None else True)
      if RegexMatch==True:
        FoundExecMode=ExecMode
        break
    if FoundExecMode!=None:
      break
  KeyWord=Sql.split(" ")[0]
  return FoundExecMode,KeyWord

# ---------------------------------------------------------------------------------------------------------------------
# Split sql statements
# ---------------------------------------------------------------------------------------------------------------------
def SplitSqlStatements(Script):
  
  #Split statements with snowflake splitter
  #(limitation: it does not join BEGIN...END blocks into a single statement)
  Statements=SnowflakeSplitStatements(io.StringIO(Script),remove_comments=True)
  Statements=[Statement[0] for Statement in Statements if len(Statement[0].strip())!=0]
  
  #Calculate nesting level of each statement
  NestingLevel=0
  NestedStatements=[]
  for Statement in Statements:
    if Statement.upper().startswith("BEGIN"):
      NestingLevel+=1
      NestedStatements.append({"level":NestingLevel,"sentence":Statement})
    elif Statement.upper().startswith("END;"):
      NestedStatements.append({"level":NestingLevel,"sentence":Statement})
      NestingLevel-=1
    else:
      NestedStatements.append({"level":NestingLevel,"sentence":Statement})

  #Join BEGIN...END blocks into a single statement
  NestingLevel=0
  PrevNestingLevel=-1
  JoinStatement=""
  JoinedStatements=[]
  for Statement in NestedStatements:
    NestingLevel=Statement["level"]
    Sentence=Statement["sentence"]
    if NestingLevel!=PrevNestingLevel:
      if len(JoinStatement)!=0:
        JoinedStatements.append(JoinStatement)
      JoinStatement=Sentence
    else:
      if NestingLevel==0:
        if len(JoinStatement)!=0:
          JoinedStatements.append(JoinStatement)
        JoinStatement=Sentence
      else:
        JoinStatement+="\n"+Sentence
    PrevNestingLevel=NestingLevel
  if len(JoinStatement)!=0:
    JoinedStatements.append(JoinStatement)
  
  #Return statements
  return JoinedStatements    

# ---------------------------------------------------------------------------------------------------------------------
# Get queries inside files
# ---------------------------------------------------------------------------------------------------------------------
def GetQueriesInFiles(FileNames,ConnectionName,Config):

  #Initialize output
  FileQueries=[]
  
  #Loop over files
  for FileName in FileNames:
    
    #Get file content
    Encoding=DetectFileEncoding(FileName)
    File=open(FileName,'r',encoding=Encoding)
    Script=File.read()
    File.close()

    #Split statements
    QueryNr=0
    Statements=SplitSqlStatements(Script)
    for Sql in Statements:
      ExecMode,KeyWord=GetQueryExecutionMode(Sql,ConnectionName,Config)
      if ExecMode==None:
        _pr.Print("Unable to find execution mode for this query:")
        CodePrint(Sql)
        return False,[]
      if ExecMode!="IGNORE":
        FileQueries.append({"file_name":FileName,"index":QueryNr,"sql":Sql,"exec_mode":ExecMode,"keyword":KeyWord})
        QueryNr+=1
  
  #Return result
  return True,FileQueries

# ---------------------------------------------------------------------------------------------------------------------
# Expands macros of the form name(arg1,arg2,...) using the given macro dictionary.
# Placeholders $1, $2, ... are replaced with the provided arguments.
# ---------------------------------------------------------------------------------------------------------------------
def ExpandMacros(Query,Macros):
  global _ColumnFormats
  _ColumnFormats=None
  Pattern=re.compile(r"(\w+)\(([^)]*)\)")
  def Replacer(Match):
    global _ColumnFormats
    Name=Match.group(1)
    Args=[A.strip() for A in Match.group(2).split(":")]
    if Name not in Macros:
      return Match.group(0)
    Template=Macros[Name]["mdef"]
    if _ColumnFormats==None and Macros[Name]["kind"]=="sql" and "column_formats" in Macros[Name]:
      _ColumnFormats=Macros[Name]["column_formats"]
    for Index,Arg in enumerate(Args):
      Template=Template.replace(f"${Index+1}",Arg)
    return Template
  NewQuery=Pattern.sub(Replacer,Query)
  Indentations=[len(Line)-len(Line.lstrip(" ")) for Line in NewQuery.split("\n")]
  Indentations=[Indentation for Indentation in Indentations if Indentation!=0]
  MinIndentation=(min(Indentations) if len(Indentations)!=0 else 0)
  NewQuery="\n".join([(Line[MinIndentation:] if len(Line[0:MinIndentation].lstrip(" "))==0 else Line) for Line in NewQuery.split("\n")])
  return NewQuery,_ColumnFormats

# ---------------------------------------------------------------------------------------------------------------------
# Execute query
# ---------------------------------------------------------------------------------------------------------------------
def ExecuteQuery(SqlQuery,ConnectionName,ConnectionsFile,Config,ExecMode="EXECUTE",ShowMode=False,DebugMode=False,PayloadMode=False):
  
  #Get query
  Query=SqlQuery
  
  #Prepare script: replace quotes and environment tags and discard use statements
  if PayloadMode==True:
    if len(Config["connections"][ConnectionName]["payload_wrapper"])==0 or Config["connections"][ConnectionName]["payload_wrapper"] not in Config["payload_wrappers"]:
      Message=f"Connection {ConnectionName} does not have a valid payload wrapper"
      return False,Message,None,None,None,None
    PayloadWrapper=Config["payload_wrappers"][Config["connections"][ConnectionName]["payload_wrapper"]]
    Query=Query.replace("'","''").replace("\\","\\\\")  
    Query=PayloadWrapper.replace("<query>",Query)
    WrappedMode=True
  elif ExecMode==None or ExecMode=="EXECUTE":
    WrappedMode=False
  elif ExecMode.startswith("WRAPPER="):
    WrapperProc=ExecMode.replace("WRAPPER=","")
    Query=Query.replace("'","''").replace("\\","\\\\")  
    Query=f"CALL {WrapperProc}('{Query}')"
    WrappedMode=True
  
  #Execute jinja2 templates
  for TemplateName in Config["connections"][ConnectionName]["jinja2_templates"]:
    TemplateDef={}
    for Item in Config["jinja2_templates_def"][TemplateName]:
      Variable=Item["variable"]
      Value=Item["value"]
      if Value.startswith("@"):
        Value=Config["connections"][ConnectionName][Value[1:]]
      TemplateDef[Variable]=Value
  Jinja2Template=jinja2.Template(Query)
  Query=Jinja2Template.render(**TemplateDef)

  #Display query in show mode
  if ShowMode==True:
    _pr.Print(Query)
    return True,"",WrappedMode,None,None,None

  #Ask before executing in debug mode
  if DebugMode==True:
    _pr.Print("Query about to execute:")
    _pr.Print(Query)
    Answer=input(f"Execute on connection {ConnectionName} (y/n) ?")
    if Answer!="y":
      Message=f"Query execution aborted by user"
      return False,Message,WrappedMode,None,None,None

  #Open connection
  try:
    with contextlib.redirect_stdout(io.StringIO()):
      Conn=SnowflakeConnect(connections_file_path=pathlib.Path(ConnectionsFile),connection_name=ConnectionName,insecure_mode=True)
  except Exception as Ex:
    Message=f"Cannot open connection to snowflake: {str(Ex)}"
    return False,Message,WrappedMode,None,None,None

  #Execute query
  try:
    Cursor=Conn.cursor()
    Cursor.execute(Query)
    Result=Cursor.fetchall()
    ColMetaData=Cursor.description
  except KeyboardInterrupt as Ex:
    Message=f"Interrupted by user"
    return False,Message,WrappedMode,None,None,None
  except Exception as Ex:
    Message=f"Execution error: {str(Ex)}"
    return False,Message,WrappedMode,Query,None,None
  if WrappedMode==True and Result[0][0]!="DONE":
    Message=Result[0][0]
    return False,Message,WrappedMode,Query,None,None
  
  #Closing connection
  Conn.close()

  #Return success
  return True,"",WrappedMode,None,Result,ColMetaData

# ---------------------------------------------------------------------------------------------------------------------
# Get objects in schema
# ---------------------------------------------------------------------------------------------------------------------
def GetObjectsInSchema(Schema,ConnectionName,ConnectionsFile,Config):
  
  #Initialize output
  Objects=[]
  
  #Get current database
  if Schema.find(".")!=-1:
    CurrDatabase=Schema.split(".")[0]
    CurrSchema=Schema.split(".")[1]
  else:
    _pr.Print("Getting current database ...",Volatile=True)
    Sql="SELECT CURRENT_DATABASE() AS CURR_DBNAME"
    Status,Message,_,_,Result,_=ExecuteQuery(Sql,ConnectionName,ConnectionsFile,Config)
    if Status==False:
      Message="Unable to get current database: "+Message
      return False,Message,None
    CurrDatabase=Result[0][0]
    CurrSchema=Schema

  #Get tables/views
  _pr.Print(f"Getting tables/views in schema {CurrDatabase}.{CurrSchema} ...",Volatile=True)
  Sql="SHOW OBJECTS IN SCHEMA <database>.<schema>".replace("<database>",CurrDatabase).replace("<schema>",CurrSchema)
  Status,Message,_,_,Result,ColMetaData=ExecuteQuery(Sql,ConnectionName,ConnectionsFile,Config)
  if Status==False:
    Message=f"Unable to get tables/views in schena {CurrSchema}: {Message}"
    return False,Message,None
  Results=[{ColMetaData[Index].name:Field for Index,Field in enumerate(Row)} for Row in Result]
  for Row in Results:
    ObjectKind=Row["kind"]
    ObjectDatabase=Row["database_name"]
    ObjectSchema=Row["schema_name"]
    ObjectName=Row["name"]
    ObjectFullName=ObjectDatabase+"."+ObjectSchema+"."+ObjectName
    if ObjectKind in ["TABLE","VIEW"] and ObjectDatabase==CurrDatabase and ObjectSchema==CurrSchema:
      Objects.append({"kind":ObjectKind,"name":ObjectFullName})
  
  #Get procedures
  _pr.Print(f"Getting procedures in schema {CurrDatabase}.{CurrSchema} ...",Volatile=True)
  Sql="SHOW PROCEDURES IN SCHEMA <database>.<schema>".replace("<database>",CurrDatabase).replace("<schema>",CurrSchema)
  Status,Message,_,_,Result,ColMetaData=ExecuteQuery(Sql,ConnectionName,ConnectionsFile,Config)
  if Status==False:
    Message=f"Unable to get procedures in schena {CurrSchema}: {Message}"
    return False,Message,None
  Results=[{ColMetaData[Index].name:Field for Index,Field in enumerate(Row)} for Row in Result]
  for Row in Results:
    ObjectBuiltIn=Row["is_builtin"]
    ObjectDatabase=Row["catalog_name"]
    ObjectSchema=Row["schema_name"]
    ObjectName=Row["arguments"].split("RETURN")[0].replace("DEFAULT ","").replace(", ",",")
    ObjectFullName=ObjectDatabase+"."+ObjectSchema+"."+ObjectName
    if ObjectBuiltIn=="N" and ObjectDatabase==CurrDatabase and ObjectSchema==CurrSchema:
      Objects.append({"kind":"PROCEDURE","name":ObjectFullName})
  
  #Get functions
  _pr.Print(f"Getting functions in schema {CurrDatabase}.{CurrSchema} ...",Volatile=True)
  Sql="SHOW FUNCTIONS IN SCHEMA <database>.<schema>".replace("<database>",CurrDatabase).replace("<schema>",CurrSchema)
  Status,Message,_,_,Result,ColMetaData=ExecuteQuery(Sql,ConnectionName,ConnectionsFile,Config)
  if Status==False:
    Message=f"Unable to get functions in schena {CurrSchema}: {Message}"
    return False,Message,None
  Results=[{ColMetaData[Index].name:Field for Index,Field in enumerate(Row)} for Row in Result]
  for Row in Results:
    ObjectBuiltIn=Row["is_builtin"]
    ObjectDatabase=Row["catalog_name"]
    ObjectSchema=Row["schema_name"]
    ObjectName=Row["arguments"].split("RETURN")[0].replace("DEFAULT ","").replace(", ",",")
    ObjectFullName=ObjectDatabase+"."+ObjectSchema+"."+ObjectName
    if ObjectBuiltIn=="N" and ObjectDatabase==CurrDatabase and ObjectSchema==CurrSchema:
      Objects.append({"kind":"FUNCTION","name":ObjectFullName})
  
  #Get tasks
  _pr.Print(f"Getting tasks in schema {CurrDatabase}.{CurrSchema} ...",Volatile=True)
  Sql="SHOW TASKS IN SCHEMA <database>.<schema>".replace("<database>",CurrDatabase).replace("<schema>",CurrSchema)
  Status,Message,_,_,Result,ColMetaData=ExecuteQuery(Sql,ConnectionName,ConnectionsFile,Config)
  if Status==False:
    Message=f"Unable to get tasks in schena {CurrSchema}: {Message}"
    return False,Message,None
  Results=[{ColMetaData[Index].name:Field for Index,Field in enumerate(Row)} for Row in Result]
  for Row in Results:
    ObjectDatabase=Row["database_name"]
    ObjectSchema=Row["schema_name"]
    ObjectName=Row["name"]
    ObjectFullName=ObjectDatabase+"."+ObjectSchema+"."+ObjectName
    if ObjectDatabase==CurrDatabase and ObjectSchema==CurrSchema:
      Objects.append({"kind":"TASK","name":ObjectFullName})
  
  #Return success
  return True,"",Objects

# ---------------------------------------------------------------------------------------------------------------------
# Project lane copy files
# ---------------------------------------------------------------------------------------------------------------------
def ProjectLaneCopyFiles(SourLanePath,DestLanePath,Config,Verbose,GitRepoPath,FilePattern):
  
  #Get files selected file copy mode
  SelSourFiles=[]
  for Folder in Config["folders"]:
    for RootFolder,_,Files in os.walk(os.path.join(SourLanePath,Folder)):
      for FileName in Files:
        FullName=AbsPath(os.path.join(RootFolder,FileName))
        if FilePattern.find("*")!=-1 or FilePattern.find("?")!=-1:
          if fnmatch.fnmatch(FullName.replace(GitRepoPath,""),FilePattern)==True:
            SelSourFiles.append(FullName)
        else:
          if FullName.find(AbsPath(FilePattern))!=-1:
            SelSourFiles.append(FullName)
  if Verbose==True:
    _pr.Print(f"Selected files from source lane: {len(SelSourFiles)} files")

  #Copy source lane into destination lane
  CopiedFiles=0
  for SourFile in SelSourFiles:
    DestFile=SourFile.replace(SourLanePath,DestLanePath)
    if DestinUpdate==True:
      try:
        os.makedirs(os.path.dirname(DestFile),exist_ok=True)
        shutil.copy2(SourFile,DestFile)
        CopiedFiles+=1
      except Exception as Ex:
        SourFile=SourFile.replace(GitRepoPath+os.sep,"")
        DestFile=DestFile.replace(GitRepoPath+os.sep,"")
        Message=f"Failed to copy file {SourFile} as {DestFile}: {Ex}"
        return False,Message
  if Verbose==True:
    _pr.Print(f"Copied files on destination lane: {CopiedFiles} files")

  #return success
  return True,""

# ---------------------------------------------------------------------------------------------------------------------
# Project lane Copy changes
# ---------------------------------------------------------------------------------------------------------------------
def ProjectLaneCopyChanges(SourLanePath,DestLanePath,Config,Verbose,GitRepoPath):
  
  #Get files selected for change copy mode
  GitFiles=[]
  SelSourFiles=[]
  RetCode,Output=Exec("git status --untracked-files=all --porcelain=v1 --ignored")
  if RetCode!=0:
    Message=f"Unable to get changed files on repo: {Output.replace("\n"," ")}"
    return False,Message
  for File in Output.strip("\n").split("\n"):
    FullName=AbsPath(File[3:])
    GitFiles.append(FullName)
  for Folder in Config["folders"]:
    for RootFolder,_,Files in os.walk(os.path.join(SourLanePath,Folder)):
      for FileName in Files:
        FullName=AbsPath(os.path.join(RootFolder,FileName))
        if FullName in GitFiles:
          SelSourFiles.append(FullName)
  if Verbose==True:
    _pr.Print(f"Selected files from source lane: {len(SelSourFiles)} files")
  
  #Copy source lane into destination lane
  CopiedFiles=0
  for SourFile in SelSourFiles:
    DestFile=SourFile.replace(SourLanePath,DestLanePath)
    if DestinUpdate==True:
      try:
        os.makedirs(os.path.dirname(DestFile),exist_ok=True)
        shutil.copy2(SourFile,DestFile)
        CopiedFiles+=1
      except Exception as Ex:
        SourFile=SourFile.replace(GitRepoPath+os.sep,"")
        DestFile=DestFile.replace(GitRepoPath+os.sep,"")
        Message=f"Failed to copy file {SourFile} as {DestFile}: {Ex}"
        return False,Message
  if Verbose==True:
    _pr.Print(f"Copied files on destination lane: {CopiedFiles} files")

  #return success
  return True,""

# ---------------------------------------------------------------------------------------------------------------------
# Project lane Copy different files
# ---------------------------------------------------------------------------------------------------------------------
def ProjectLaneCopyDiff(SourLanePath,DestLanePath,Config,Verbose,GitRepoPath,DiffBranch):
  
  #Get files selected for change copy mode
  GitFiles=[]
  SelSourFiles=[]
  RetCode,Output=Exec(f"git diff --name-status {DiffBranch}..HEAD --diff-filter=d")
  if RetCode!=0:
    Message=f"Unable to get different files compared to {DiffBranch} branch on repo: {Output.replace("\n"," ")}"
    return False,Message
  for DiffFile in Output.strip("\n").split("\n"):
    Status=DiffFile.split("\t")[0][0]
    if Status in ["R","C"]:
      FileName=DiffFile.split("\t")[2]
    else:
      FileName=DiffFile.split("\t")[1]
    FullName=AbsPath(DiffFile)
    GitFiles.append(FullName)
  for Folder in Config["folders"]:
    for RootFolder,_,Files in os.walk(os.path.join(SourLanePath,Folder)):
      for FileName in Files:
        FullName=AbsPath(os.path.join(RootFolder,FileName))
        if FullName in GitFiles:
          SelSourFiles.append(FullName)
  if Verbose==True:
    _pr.Print(f"Selected files from source lane: {len(SelSourFiles)} files")
  
  #Copy source lane into destination lane
  CopiedFiles=0
  for SourFile in SelSourFiles:
    DestFile=SourFile.replace(SourLanePath,DestLanePath)
    if DestinUpdate==True:
      try:
        os.makedirs(os.path.dirname(DestFile),exist_ok=True)
        shutil.copy2(SourFile,DestFile)
        CopiedFiles+=1
      except Exception as Ex:
        SourFile=SourFile.replace(GitRepoPath+os.sep,"")
        DestFile=DestFile.replace(GitRepoPath+os.sep,"")
        Message=f"Failed to copy file {SourFile} as {DestFile}: {Ex}"
        return False,Message
  if Verbose==True:
    _pr.Print(f"Copied files on destination lane: {CopiedFiles} files")

  #return success
  return True,""

# ---------------------------------------------------------------------------------------------------------------------
# Project lane copy full
# ---------------------------------------------------------------------------------------------------------------------
def ProjectLaneCopyFull(SourLanePath,DestLanePath,Config,Verbose,DestinUpdate):

  #Delete all files in destination lane directory
  DeletedFiles=0
  DeletedFolders=0
  for Folder in Config["folders"]:
    FolderPath=os.path.join(DestLanePath,Folder)
    if os.path.exists(FolderPath):
      for File in os.listdir(FolderPath):
        Path=os.path.join(FolderPath,File)
        if os.path.isfile(Path):
          if DestinUpdate==True:
            try:
              os.remove(Path)
              DeletedFiles+=1
            except Exception as Ex:
              Message=f"Failed to delete file ({Path}: {Ex}"
              return False,Message
        elif os.path.isdir(Path):
          if DestinUpdate==True:
            try:
              shutil.rmtree(Path)
              DeletedFolders+=1
            except Exception as Ex:
              Message=f"Failed to delete folder ({Path}): {Ex}"
              return False,Message
  if Verbose==True:
    _pr.Print(f"Deleted {DeletedFiles} files and {DeletedFolders} folders in destination lane")
  if DestinUpdate==True:
    FileCount=CountFiles(DestLanePath,Config["folders"])
    if FileCount!=0:
      Message=f"Unable to delete all files on destination lane, file count is still {FileCount}"
      return False,Message

  #Copy source lane into destination lane
  CopiedDirs=0
  for Folder in Config["folders"]:
    try:
      SourPath=os.path.join(SourLanePath,Folder)
      DestPath=os.path.join(DestLanePath,Folder)
      if DestinUpdate==True:
        os.makedirs(os.path.dirname(DestPath),exist_ok=True)
        shutil.copytree(SourPath,DestPath,dirs_exist_ok=True)  
        CopiedDirs+=1
    except Exception as Ex:
      Message=f"Failed to copy files from source lane into destination lane: {Ex}"
      return False,Message
  if Verbose==True:
    _pr.Print(f"Copied folders on destination lane: {CopiedDirs} folders")

  #return success
  return True,""

# ---------------------------------------------------------------------------------------------------------------------
# Project lane replicate
# ---------------------------------------------------------------------------------------------------------------------
def ProjectLaneReplicate(RunMode,GitRepoPath,SourceLane,DestinLane,FilePattern,DestinUpdate,DiffBranch,Config,ForbiddenSchemas,IgnoreSchemaCheck,Verbose=True,ReplicFiles=None):

  #Message
  if Verbose==True:
    if RunMode=="REPL-FULL":
      _pr.Print(f"Doing full copy from lane {SourceLane} to {DestinLane} ...")
    elif RunMode=="REPL-CHANGES":
      _pr.Print(f"Doing git changes only copy from lane {SourceLane} to {DestinLane} ...")
    elif RunMode=="REPL-DIFF":
      _pr.Print(f"Doing git diff changes only copy from lane {SourceLane} to {DestinLane} ...")
    elif RunMode=="REPL-FILE":
      _pr.Print(f"Doing selected file from lane {SourceLane} to {DestinLane} using file pattern {FilePattern} ...")
  
  #Get file paths on configuration
  if SourceLane not in Config["lanes"]:
    Message=f"Source lane ({SourceLane}) is not in the configuration file"
    return False,Message
  if DestinLane not in Config["lanes"]:
    Message=f"Destination lane ({DestinLane}) is not in the configuration file"
    return False,Message
  SourLanePath=AbsPath(GitRepoPath+os.sep+Config["lanes"][SourceLane]["path"]+os.sep)
  DestLanePath=AbsPath(GitRepoPath+os.sep+Config["lanes"][DestinLane]["path"]+os.sep)
  if not os.path.exists(SourLanePath):
    Message=f"Source lane path ({SourLanePath}) does not exist"
    return False,Message
  if not os.path.exists(DestLanePath):
    os.makedirs(DestLanePath,exist_ok=True)
    Message=f"Destination lane path ({DestLanePath}) created"
    _pr.Print(Message)
  if Verbose==True:
    _pr.Print(f"Source lane [{SourceLane}]: {CountFiles(SourLanePath,Config["folders"])} files")
    _pr.Print(f"Destin lane [{DestinLane}]: {CountFiles(DestLanePath,Config["folders"])} files")

  #Modification warning
  if DestinUpdate==True and Config["lanes"][DestinLane]["modify_warning"]==1:
    UserInput=input(f"Source code lane \"{DestinLane}\" is going to be modified. Continue [y/n] ?")
    if UserInput!="y":
      Message=f"Operation aborted by user"
      return False,Message

  #Copy files in file copy mode
  if RunMode=="REPL-FILE":
    Status,Message=ProjectLaneCopyFiles(SourLanePath,DestLanePath,Config,Verbose,GitRepoPath,FilePattern)
    if Status==False:
      return False,Message
  
  #Copy files in changes copy mode
  elif RunMode=="REPL-CHANGES":
    Status,Message=ProjectLaneCopyChanges(SourLanePath,DestLanePath,Config,Verbose,GitRepoPath)
    if Status==False:
      return False,Message

  #Copy files in diff copy mode
  elif RunMode=="REPL-DIFF":
    Status,Message=ProjectLaneCopyDiff(SourLanePath,DestLanePath,Config,Verbose,GitRepoPath,DiffBranch)
    if Status==False:
      return False,Message

  #Copy files in full copy mode
  elif RunMode=="REPL-FULL":
    Status,Message=ProjectLaneCopyFull(SourLanePath,DestLanePath,Config,Verbose,DestinUpdate)
    if Status==False:
      return False,Message

  #Rename files on destination lane
  FilesRenamed=0
  SelDestFiles=[]
  for Folder in Config["folders"]:
    for RootFolder,_,Files in os.walk(os.path.join(DestLanePath,Folder)):
      for FileName in Files:
        WorkFile=FileName
        SourSuffix=Config["lanes"][SourceLane]["file_suffix"]
        DestSuffix=Config["lanes"][DestinLane]["file_suffix"]
        if SourSuffix!="":
          FileNameNoExt,Ext=os.path.splitext(WorkFile)
          FileNameNoExt=FileNameNoExt.replace("_"+SourSuffix,"")
          WorkFile=FileNameNoExt+Ext
        if DestSuffix!="":
          FileNameNoExt,Ext=os.path.splitext(WorkFile)
          if FileNameNoExt.endswith("_"+DestSuffix)==False:
            WorkFile=FileNameNoExt+"_"+DestSuffix+Ext
          else:
            WorkFile=FileNameNoExt+Ext
        OldName=os.path.join(RootFolder,FileName)
        NewName=os.path.join(RootFolder,WorkFile)
        if OldName!=NewName:
          if DestinUpdate==True:
            try:
              os.replace(OldName,NewName)
              SelDestFiles.append(NewName)
              FilesRenamed+=1
            except Exception as Ex:
              OldName=OldName.replace(GitRepoPath+os.sep,"")
              NewName=NewName.replace(GitRepoPath+os.sep,"")
              Message=f"Failed to rename file {OldName} as {NewName}: {Ex}"
              return False,Message
  if Verbose==True:
    _pr.Print(f"Files renamed on destination lane: {FilesRenamed} files")

  #Apply transslation rules
  FilesCopied=0
  FilesModified=0
  TotalChanges=0
  for FileName in SelDestFiles:
        
    #Detect file encoding
    BinaryFile=DetectBinaryFile(FileName)
    if BinaryFile==True:
      if Verbose==True:
        _pr.Print("Ignoring binary file "+FileName)
      continue

    #Read file
    try:
      Encoding=DetectFileEncoding(FileName)
      File=open(FileName,"r",newline='',encoding=Encoding)
      Lines=[Line.rstrip("\r\n") for Line in File]
      File.close()
    except Exception as Ex:
      Message=f"Unable to read file ({FileName.replace(GitRepoPath+os.sep,"")}): {Ex}"
      return False,Message
    
    #Do translation rules
    FileChanges=0
    for Tag in Config["tags"]:
      TagName=Tag["name"]
      Replacements=Tag["replacements"]
      for Replacement in Replacements:
        for Rule in Config["rules"]:
          Scope=Rule["scope"]
          SearchStr=Rule[SourceLane].replace(TagName,Replacement)
          ReplaceStr=Rule[DestinLane].replace(TagName,Replacement)
          if fnmatch.fnmatch(FileName.replace(GitRepoPath,""),Scope)==True:
            for i in range(len(Lines)):
              NewLine=Lines[i].replace(SearchStr,ReplaceStr)
              if NewLine!=Lines[i]:
                FileChanges+=1
              Lines[i]=NewLine
    if FileChanges!=0:
      FilesModified+=1
      TotalChanges+=FileChanges
    else:
      if FileName.lower().endswith(EXEC_SCRIPT_EXT)==True:
        Message=f"Executable file {FileName.replace(GitRepoPath+os.sep,"")} did not produce any replacements! Source code lanes probably not configured correctly!"
        return False,Message
    
    #Safe check of forbidden schemas in resulting file
    if FileName.lower().endswith(EXEC_SCRIPT_EXT)==True and IgnoreSchemaCheck==False:
      for i in range(len(Lines)):
        for ForbiddenSchema in ForbiddenSchemas:
          if re.search(r'\b'+ForbiddenSchema+r'\b',Lines[i],re.IGNORECASE)!=None:
            Message=f"Code replication failure: Line {i} in file ({FileName.replace(GitRepoPath+os.sep,"")}) contains forbidden schema {ForbiddenSchema} after replacements"
            Message+="\n"+FileName.replace(GitRepoPath+os.sep,"")+":"
            Message+="\n"+str(i)+": "+Lines[i]
            return False,Message

    #Write file
    if DestinUpdate==True:
      try:
        File=open(FileName,"w",encoding=Encoding)
        File.write("\n".join(Lines))
        File.close()
      except Exception as Ex:
        Message=f"Unable to write file ({FileName.replace(GitRepoPath+os.sep,"")}): {Ex}"
        return False,Message
      FilesCopied+=1

  #Return replicated files
  if ReplicFiles!=None:
    for FileName in SelDestFiles:
      ReplicFiles.append(FileName.replace(GitRepoPath+os.sep,""))

  #Message
  if Verbose==True:
    _pr.Print(f"Files copied:{FilesCopied}, Files modified:{FilesModified}, Total changes:{TotalChanges}")
  
  #Return success
  return True,""

# ---------------------------------------------------------------------------------------------------------------------
# Create resources for testing
# ---------------------------------------------------------------------------------------------------------------------
def CreateTestResources(Path,Config,IgnoreSchemaCheck):

  #Check source code lanes are defined
  if Config==None:
    Message="Source code lanes are not defined in configuration file!"
    return False,Message,None

  #Detect if passed resource id a file or folder
  if os.path.isdir(Path):
    Folder=Path
    ReplicMode="REPL-FULL"
    ReplicFile=None
  else:
    Folder=os.path.dirname(Path)
    ReplicMode="REPL-FILE"
    ReplicFile=Path
  
  #Detect if we are inside git repo
  GitRepoPath=GetGitRepoTopLevel(Folder)
  if GitRepoPath==None:
    Message=f"Directory of path {Path} is not a git repository!"
    return False,Message,None

  #Check tool is executed at the repo top level
  if GitRepoPath!=AbsPath(os.getcwd()):
    Message="Current directory is not the top level of current git repository, change current directory!"
    return False,Message,None

  #Get configuration for this repo
  if GitRepoPath in Config:
    Config=Config[GitRepoPath]
  else:
    Message=f"Git repository ({GitRepoPath}) does not appear in configuration file!"
    return False,Message,None

  #Identify project lane of passed file
  SourceLane=None
  for Lane in Config["lanes"]:
    LanePath=AbsPath(GitRepoPath+os.sep+Config["lanes"][Lane]["path"]+os.sep)
    if AbsPath(Path).find(LanePath)!=1:
      SourceLane=Lane
  if SourceLane==None:
    Message=f"Cannot identify source code lane of path {Path}"
    return False,Message,None

  #Get corresponding test lane (destination)
  DestinLane=Config["lanes"][SourceLane]["testing_lane"]
  if len(DestinLane)==0:
    Message=f"Unable determina testing source code lane of path {Path}"
    return False,Message,None

  #Get forbidden schemas
  ForbiddenSchemas=[]
  for Lane in Config["lanes"]:
    if Lane!=DestinLane:
      ForbiddenSchemas.extend(Config["lanes"][Lane]["used_schemas"])

  #Do project replication for single file
  ReplicFiles=[]
  Status,Message=ProjectLaneReplicate(ReplicMode,GitRepoPath,SourceLane,DestinLane,ReplicFile,True,None,Config,ForbiddenSchemas,IgnoreSchemaCheck,False,ReplicFiles)
  if Status==False:
    return False,Message,None
  
  #Check replicated files
  if ReplicMode=="REPL-FILE" and len(ReplicFiles)!=1:
    Message=f"Replication of file {os.path.basename(Path)} on testing lane failed, replication returned {len(ReplicFiles)} file(s)\nReview source code lane configuration if new folders or files were added recently."
    return False,Message,None

  #Get result
  if ReplicMode=="REPL-FILE":
    Result=ReplicFiles[0]
  else:
    Result=AbsPath(Config["lanes"][DestinLane]["path"]).replace(GitRepoPath+os.sep,"")

  #Return success
  return True,"",Result

# ---------------------------------------------------------------------------------------------------------------------
# Main process
# ---------------------------------------------------------------------------------------------------------------------
def RunModeMacros(RunMode,MacroFilter,Config):
  
  #Check macros are defined
  if Config==None:
    _pr.Print("Macros are not defined in configuration file!")
    return False
  
  #List macros without detail
  if RunMode=="MACRO-LIST":
    ColumnNames=["Macro","Description","Kind","Arg","Argument descriptions"]
    ColumnFormats=["L","L","L","C","L"]
    RowData=[[
      Macro,
      Config[Macro]["description"],
      Config[Macro]["kind"],
      (str(len(Config[Macro]["args"])) if len(Config[Macro]["args"])!=0 else '-'),
      ", ".join(["$"+str(Index+1)+"="+Arg for Index,Arg in enumerate(Config[Macro]["args"])])
    ] for Macro in Config]
    _pr.Print("",Volatile=True)
    _pr.PrintTable(ColumnNames,None,ColumnFormats,RowData)

  #List macros full details
  elif RunMode=="MACRO-DETAIL":

    #Loop over macros
    OnePrinted=False
    for Macro in Config:

      #Filter by macro filter
      if fnmatch.fnmatch(Macro,MacroFilter)==False:
        continue
      else:
        OnePrinted=True
      
      #Get macro details
      Name=Macro
      Kind=Config[Macro]["kind"]
      Desc=Config[Macro]["description"]
      Args=Config[Macro]["args"]
      Def=Config[Macro]["mdef"]

      #Re-indent query
      if len(Def.split("\n"))>1:
        LastLine=Def.split("\n")[-1]
        Indentation=len(LastLine)-len(LastLine.lstrip(" "))
        Def=Def.replace(" "*Indentation," "*14)
      
      #_pr.Print macro
      print()
      print("Macro       : "+Name+("("+",".join(["$"+str(Index+1) for Index,Arg in enumerate(Args)])+")"))
      print("Kind        : "+Kind)
      print("Description : "+Desc)
      if len(Args)!=0:
        print("Arguments   : ",end="",flush=True)
        for Index,Arg in enumerate(Args):
          if Index!=0:
            print("              ",end="",flush=True)
          print(f"${Index+1}={Arg}")
      print("Definition  : "+Def)
    
    #Warning nothing selected
    if OnePrinted==False:
      print("Macro filter given did not select any macro names")

  #Return success
  return True

# ---------------------------------------------------------------------------------------------------------------------
# Run SQL query mode
# ---------------------------------------------------------------------------------------------------------------------
def RunModeSqlQuery(Connections,ConnectionsFile,SqlQuery,DisplayTypes,CombineResults,CsvOutput,Config,MacrosConfig,ShowMode,DebugMode,PayloadMode):
  
  #Import snowflake libraries
  _pr.Print("Importing snowflake libraries ...",Volatile=True)
  Status,Message=ImportSnowflakeLibraries()
  if Status==False:
    print(Message)
    return False

  #Complete macro replacements
  SqlQueryAfterMacros,MacroColumnFormats=ExpandMacros(SqlQuery,MacrosConfig)

  #Split statements
  if re.search(r'\bBEGIN\b',SqlQueryAfterMacros,re.IGNORECASE):
    Statements=[SqlQueryAfterMacros]
  else:
    Statements=SplitSqlStatements(SqlQueryAfterMacros)

  #Execute statements
  StartTime=datetime.datetime.now()
  _pr.Print(f"Executing {"(payload) " if PayloadMode==True else ""}...",Volatile=True)
  for Sql in Statements:
    
    #Loop over connections
    OutputResults=[]
    for ConnectionName in Connections.split(","):
      
      #Execute query
      ExecMode,KeyWord=GetQueryExecutionMode(Sql,ConnectionName,Config)
      if ExecMode=="IGNORE":
        continue
      Status,Message,WrappedMode,Query,Result,ColMetaData=ExecuteQuery(Sql,ConnectionName,ConnectionsFile,Config,ExecMode,ShowMode,DebugMode,PayloadMode)
      if Status==False:
        _pr.Print(f"[ERROR] Execution failed: "+Message)
        if Query!=None:
          _pr.Print("Passed query:")
          CodePrint(Query)
        return False
      if ShowMode==True:
        continue
      if WrappedMode==True:
        _pr.Print("[OK] Execution was completed successfully")
        continue

      #Get results when output is data types
      if DisplayTypes==True:
        OutputRows=len(Result)
        ColumnNames=["Name","TypeName","DisplaySize","InternalSize","Precision","Scale","IsNullable"]
        ColumnFormats=["L","L","R","R","R","R","C"]
        Result=[]
        for Col in ColMetaData:
          ColName=str(Col.name)
          TypeName=(SNOWFLAKE_TYPE_CODES[Col.type_code] if Col.type_code in SNOWFLAKE_TYPE_CODES else "(unknown)")
          DisplaySize=("" if Col.display_size==None else str(Col.display_size))
          InternalSize=("" if Col.internal_size==None else str(Col.internal_size))
          Precision=("" if Col.precision==None else str(Col.precision))
          Scale=("" if Col.scale==None else str(Col.scale))
          IsNullable=("Yes" if Col.is_nullable==True else "-")
          Result.append([ColName,TypeName,DisplaySize,InternalSize,Precision,Scale,IsNullable])
        if CombineResults==True and len(Connections.split(","))>1:
          OutputResults.append({"connection_name":ConnectionName,"column_names":ColumnNames,"column_types":None,"column_formats":ColumnFormats,"row_data":Result,"row_count":OutputRows})
        else:
          _pr.Print("",Volatile=True)
          PrintResults(ColumnNames,None,ColumnFormats,Result,CsvOutput)

      #Get results when output is rows
      else:
        if Result!=None:
          Result=[[str(Column).replace("\r","") for Column in Row] for Row in Result]
        if len(Result)==0:
          Result=[["" for Col in ColMetaData]]
        OutputRows=len(Result)
        ColumnNames=[Col.name for Col in ColMetaData]
        ColumnTypes=[("("+SNOWFLAKE_TYPE_CODES[Col.type_code]+")" if Col.type_code in SNOWFLAKE_TYPE_CODES else "(unknown)") for Col in ColMetaData]
        if MacroColumnFormats!=None:
          ColumnFormats=MacroColumnFormats
        else:  
          ColumnFormats=[("R" if Type in ["(int)"] else "L") for Type in ColumnTypes]
          ColumnFormats[-1]="LM"
        if CombineResults==True and len(Connections.split(","))>1:
          OutputResults.append({"connection_name":ConnectionName,"column_names":ColumnNames,"column_types":ColumnTypes,"column_formats":ColumnFormats,"row_data":Result,"row_count":OutputRows})
        else:
          _pr.Print("",Volatile=True)
          PrintResults(ColumnNames,ColumnTypes,ColumnFormats,Result,CsvOutput)
          ElapsedSeconds=FormatSeconds((datetime.datetime.now()-StartTime).total_seconds())
          _pr.Print(f"{OutputRows} row(s) returned ({ElapsedSeconds})")

    #_pr.Print combined results
    if CombineResults==True and len(Connections.split(","))>1:
      
      #Exit if there is not any results
      if len(OutputResults)==0:
        return True
      
      #Check all output columns are the same
      FirstColumnNames=OutputResults[0]["column_names"]
      FirstColumnTypes=OutputResults[0]["column_types"]
      SameColumns=True
      for Output in OutputResults:
        if Output["column_names"]!=FirstColumnNames or Output["column_types"]!=FirstColumnTypes:
          _pr.Print("Cannot combine results as not all queries return same columns")
          SameColumns=False
      
      #Output not combined
      if SameColumns==False:
        TotalRows=0
        for Output in OutputResults:
          ConnectionName=Output["connection_name"]
          ColumnNames=Output["column_names"]
          ColumnTypes=Output["column_types"]
          ColumnFormats=Output["column_formats"]
          RowData=Output["row_data"]
          RowCount=Output["row_count"]
          _pr.Print("",Volatile=True)
          PrintResults(ColumnNames,ColumnTypes,ColumnFormats,RowData,CsvOutput)
          TotalRowws+=RowCount
        _pr.Print(f"{TotalRows} row(s) returned")
      
      #Output combined
      else:
        AllColumnNames=["Conn"]+OutputResults[0]["column_names"]
        AllColumnTypes=[""]+OutputResults[0]["column_types"]
        AllColumnFormats=["C"]+OutputResults[0]["column_formats"]
        AllRowData=[]
        AllRowCount=0
        for Output in OutputResults:
          ConnectionName=Output["connection_name"]
          ColumnNames=Output["column_names"]
          ColumnTypes=Output["column_types"]
          ColumnFormats=Output["column_formats"]
          RowData=Output["row_data"]
          RowCount=Output["row_count"]
          if RowCount!=0:
            if len(AllRowData)!=0:
              _pr.AddHline(AllRowData)
            AllRowData.extend([[ConnectionName,*Row] for Row in RowData])
          AllRowCount+=RowCount
        if len(AllRowData)==0:
          AllRowData=[["" for Col in ColumnNames]]
        _pr.Print("",Volatile=True)
        PrintResults(AllColumnNames,AllColumnTypes,AllColumnFormats,AllRowData,CsvOutput)
        ElapsedSeconds=FormatSeconds((datetime.datetime.now()-StartTime).total_seconds())
        _pr.Print(f"{AllRowCount} row(s) returned ({ElapsedSeconds})")
  
  #Return success
  return True

# ---------------------------------------------------------------------------------------------------------------------
# Run mode for list objects in schema
# ---------------------------------------------------------------------------------------------------------------------
def RunModeListSchema(Schema,ConnectionName,ConnectionsFile,Config):

  #Import snowflake libraries
  _pr.Print("Importing snowflake libraries ...",Volatile=True)
  Status,Message=ImportSnowflakeLibraries()
  if Status==False:
    _pr.Print(Message)
    return False

  #Get objects in schema
  Status,Message,Objects=GetObjectsInSchema(Schema,ConnectionName,ConnectionsFile,Config)
  if Status==False:
    _pr.Print(Message)
    return False

  #_pr.Print objects in schema
  if len(Objects)!=0:
    ColumnNames=["Kind","ObjectName"]
    ColumnFormats=["L","L"]
    RowData=[[Object["kind"],Object["name"]] for Object in Objects]
    _pr.Print("",Volatile=True)
    _pr.PrintTable(ColumnNames,None,ColumnFormats,RowData)
  else:
    _pr.Print(f"No objects present in schema {Schema}")
  
  #Return success
  return True
  
# ---------------------------------------------------------------------------------------------------------------------
# Run mode for drop objects in schema
# ---------------------------------------------------------------------------------------------------------------------
def RunModeDropSchema(Schema,ConnectionName,ConnectionsFile,Config):

  #Import snowflake libraries
  _pr.Print("Importing snowflake libraries ...",Volatile=True)
  Status,Message=ImportSnowflakeLibraries()
  if Status==False:
    _pr.Print(Message)
    return False

  #Get objects in schema
  Status,Message,Objects=GetObjectsInSchema(Schema,ConnectionName,ConnectionsFile,Config)
  if Status==False:
    _pr.Print(Message)
    return False

  #Exit here is schema is empty
  if len(Objects)==0:
    _pr.Print(f"Schema {Schema} is empty!")
    return True
  
  #Ask for user confirmation
  _pr.Print("",Volatile=True)
  _pr.Print(f"The following object(s) will be dropped on {Config["connections"][ConnectionName]["environment"]} environment using connection {ConnectionName.upper()}:")
  for Object in Objects:
    _pr.Print(Object["kind"]+" "+Object["name"])
  Answer=input(f"Continue (y/n) ?")
  if Answer!="y":
    Message="Action cancelled by user"
    _pr.Print(Message)
    return True
  
  #Compose SQl query
  Sql=["BEGIN"]
  for Object in Objects:
    ObjectKind=Object["kind"]
    ObjectName=Object["name"]
    Sql.append(f"DROP {ObjectKind} IF EXISTS {ObjectName};")
  Sql.append("END;")

  #Compose SQl query
  _pr.Print("Executing ... ",Partial=True)
  Query="\n".join(Sql)
  Status,Message,_,_,_,_=ExecuteQuery(Query,ConnectionName,ConnectionsFile,Config)
  if Status==False:
    _pr.Print("")
    Message=f"Unable to drop objects in schema {Schema}: {Message}"
    _pr.Print(Message)
    return False
  _pr.Print("DONE")

  #Return success
  return True

# ---------------------------------------------------------------------------------------------------------------------
# Run modes for script execution
# ---------------------------------------------------------------------------------------------------------------------
def RunModeScriptExecution(RunMode,FileName,FolderName,DiffBranch,ConnectionName,ConnectionsFile,ForceMode,IgnoreHash,ShowMode,DebugMode,IgnoreSchemaCheck,Config,ScLanesConfig=None,TestMode=False):

  #Get files for execute single script mode
  if RunMode in ["EXEC-FILE","TEST-FILE"]:
    if FileName.lower().endswith(EXEC_SCRIPT_EXT)==False:
      _pr.Print(f"Cannot execute file {FileName}, it must have {EXEC_SCRIPT_EXT} extension")
      return False
    WorkFileName=FileName
    if TestMode==True:
      Status,Message,TestFileName=CreateTestResources(WorkFileName,ScLanesConfig,IgnoreSchemaCheck)
      if Status==False:
        _pr.Print(Message)
        return False
      WorkFileName=TestFileName
    Status,Message,AlreadyExecuted,ExecTimeStamp=GetAlreadyExecuted(AbsPath(WorkFileName),ConnectionName,IgnoreHash)
    if Status==False:
      _pr.Print(f"Unable to detect if file was executed already: {Message}")
      return False
    if AlreadyExecuted==True:
      _pr.Print(f"Script has been already executed on {ExecTimeStamp}")
      return True
    TotalFiles=1
    Files=[WorkFileName]

  #Get files in folder for folder mode
  elif RunMode in ["EXEC-FOLDER","TEST-FOLDER"]:
    if not os.path.isdir(FolderName):
      _pr.Print(f"Folder {FolderName} does not exist!")
      return False
    Files=[]
    WorkFolder=FolderName
    if TestMode==True:
      Status,Message,TestFolder=CreateTestResources(WorkFolder,ScLanesConfig,IgnoreSchemaCheck)
      if Status==False:
        _pr.Print(Message)
        return False
      WorkFolder=TestFolder
    TotalFiles=0
    for Root,_,FileNames in os.walk(WorkFolder):
      for FileName in FileNames:
        FullFileName=os.path.join(Root,FileName)
        if FullFileName.lower().endswith(EXEC_SCRIPT_EXT):
          Status,Message,AlreadyExecuted,ExecTimeStamp=GetAlreadyExecuted(AbsPath(FullFileName),ConnectionName,IgnoreHash)
          if Status==False:
            _pr.Print(f"Unable to detect if file ({os.path.basename(FullFileName)}) was executed already: {Message}")
            return False
          if AlreadyExecuted==False:
            Files.append(FullFileName)
          TotalFiles+=1
    Files.sort()
    if len(Files)==0:
      _pr.Print(f"No {EXEC_SCRIPT_EXT} files found on folder {WorkFolder} or all {EXEC_SCRIPT_EXT} files were executed already")
      return True

  #Get files for execute changed files on repo mode
  elif RunMode in ["EXEC-CHANGES","TEST-CHANGES"]:
    RetCode,Changes=Exec("git status --untracked-files=all --porcelain=v1")
    if RetCode!=0:
      _pr.Print(f"Unable to check changed files or not inside a git repo!")
      return False
    Files=[]
    TotalFiles=0
    for Change in Changes.strip("\n").split("\n"):
      FileStatus=Change[:2].replace(" ","-")
      FileName=AbsPath(Change[3:])
      FileExt=os.path.splitext(FileName)[1].lower()
      if FileStatus in ["M-","A-","-M","-A","??"] and FileExt==EXEC_SCRIPT_EXT:
        WorkFileName=FileName
        if TestMode==True:
          Status,Message,TestFileName=CreateTestResources(WorkFileName,ScLanesConfig,IgnoreSchemaCheck)
          if Status==False:
            _pr.Print(Message)
            return False
          WorkFileName=TestFileName
        Status,Message,AlreadyExecuted,ExecTimeStamp=GetAlreadyExecuted(WorkFileName,ConnectionName,IgnoreHash)
        if Status==False:
          _pr.Print(f"Unable to detect if file ({os.path.basename(WorkFileName)}) was executed already: {Message}")
          return False
        if AlreadyExecuted==False:
          Files.append(WorkFileName)
        TotalFiles+=1
    Files.sort()
    if len(Files)==0:
      _pr.Print(f"No changed files detected or all changed files were executed already")
      return True

  #Get files for different files in branch comparrison
  elif RunMode in ["EXEC-DIFF","TEST-DIFF"]:
    RetCode,Diff=Exec(f"git diff --name-status {DiffBranch}..HEAD")
    if RetCode!=0:
      _pr.Print(f"Unable to get different files or not inside a git repo!")
      return False
    Files=[]
    TotalFiles=0
    for DiffFile in Diff.strip("\n").split("\n"):
      Status=DiffFile.split("\t")[0][0]
      if Status=="R":
        FileName=DiffFile.split("\t")[2]
      else:
        FileName=DiffFile.split("\t")[1]
      FileExt=os.path.splitext(FileName)[1].lower()
      if Status in ["A","M","R"] and FileExt==EXEC_SCRIPT_EXT:
        WorkFileName=FileName
        if TestMode==True:
          Status,Message,TestFileName=CreateTestResources(WorkFileName,ScLanesConfig,IgnoreSchemaCheck)
          if Status==False:
            _pr.Print(Message)
            return False
          WorkFileName=TestFileName
        Status,Message,AlreadyExecuted,ExecTimeStamp=GetAlreadyExecuted(WorkFileName,ConnectionName,IgnoreHash)
        if Status==False:
          _pr.Print(f"Unable to detect if file ({os.path.basename(WorkFileName)}) was executed already: {Message}")
          return False
        if AlreadyExecuted==False:
          Files.append(WorkFileName)
        TotalFiles+=1
    Files.sort()
    if len(Files)==0:
      _pr.Print(f"No changed files detected or all changed files were executed already")
      return True

  #Import snowfkake libraries
  Status,Message=ImportSnowflakeLibraries()
  if Status==False:
    _pr.Print(Message)
    return False

  #Get statements from files
  Status,Queries=GetQueriesInFiles(Files,ConnectionName,Config)
  if Status==False:
    return False

  #Confirmation from user about execution
  if ForceMode==False:
    UniqueFiles=collections.Counter([Query["file_name"] for Query in Queries])
    MaxFileNameLength=max([len(File) for File in UniqueFiles])
    KeyWords=",".join([Query["keyword"] for Query in Queries])
    _pr.Print(f"The following script(s) will be executed on {Config["connections"][ConnectionName]["environment"]} environment using connection {ConnectionName.upper()}:")
    for File in UniqueFiles:
      KeyWords=[]
      LastKeyWord=""
      Count=0
      for KeyWord in [Query["keyword"] for Query in Queries if Query["file_name"]==File]:
        if len(LastKeyWord)!=0 and LastKeyWord!=KeyWord:
          KeyWords.append(LastKeyWord+"["+str(Count)+"]")
          Count=0
        LastKeyWord=KeyWord
        Count+=1
      if len(LastKeyWord)!=0:
        KeyWords.append(LastKeyWord+"["+str(Count)+"]")
      _pr.Print(File.ljust(MaxFileNameLength)+" ("+str(UniqueFiles[File])+f" statement(s): {", ".join(KeyWords).replace("[1]","")})")
    _pr.Print(f"{len(Files)}(s) files selected for execution"+(f", {TotalFiles-len(Files)}(s) files ignored" if TotalFiles!=len(Files) else ""))
    Answer=input(f"Continue (y/n) ?")
    if Answer!="y":
      _pr.Print("Execution cancelled by user")
      return True

  #Execute queries
  TotalSeconds=0
  MaxLengthFileName=max([len(Query["file_name"]) for Query in Queries])
  MaxLengthIndex=max([len(str(Query["index"])) for Query in Queries])
  for Query in Queries:
    StartTime=datetime.datetime.now()
    FileName=Query["file_name"]
    Index=Query["index"]
    Sql=Query["sql"]
    ExecMode=Query["exec_mode"]
    KeyWord=Query["keyword"]
    if ShowMode==False:
      _pr.Print(f"Executing {FileName.ljust(MaxLengthFileName)} ({str(Index).ljust(MaxLengthIndex)}: {KeyWord} query) ... ",Partial=True)
      if DebugMode==True:
        print()
    Status,Message,_,Query,_,_=ExecuteQuery(Sql,ConnectionName,ConnectionsFile,Config,ExecMode,ShowMode,DebugMode)
    if Status==False:
      _pr.Print("")
      _pr.Print(f"[ERROR] Execution failed: "+Message)
      if Query!=None:
        _pr.Print("Passed query:")
        CodePrint(Query)
      return False
    if ShowMode==False:
      UpdateFileExecutionDateHash(AbsPath(FileName),ConnectionName)
      ElapsedSeconds=(datetime.datetime.now()-StartTime).total_seconds()
      TotalSeconds+=ElapsedSeconds
      _pr.Print(f"DONE ({FormatSeconds(ElapsedSeconds)})")
  if ShowMode==False:
    _pr.Print(f"[OK] All scripts executed successfully ({FormatSeconds(TotalSeconds)})")

  #Return success
  return True

# ---------------------------------------------------------------------------------------------------------------------
# Project replication modes
# ---------------------------------------------------------------------------------------------------------------------
def RunModeProjectReplication(RunMode,SourceLane,DestinLane,FilePattern,DestinUpdate,DiffBranch,IgnoreSchemaCheck,Config):

  #Check macros are defined
  if Config==None:
    _pr.Print("Source code lanes are not defined in configuration file!")
    return False

  #Detect if we are inside git repo
  GitRepoPath=GetGitRepoTopLevel()
  if GitRepoPath==None:
    _pr.Print("Current directory is not a git repository!")
    return False

  #Check tool is executed at the repo top level
  if GitRepoPath!=AbsPath(os.getcwd()):
    _pr.Print("Current directory is not the top level of current git repository!")
    return False

  #Get configuration for this repo
  if GitRepoPath in Config:
    Config=Config[GitRepoPath]
  else:
    _pr.Print("Git repository does not appear in configuration file!")
    return False

  #Get forbidden schemas
  ForbiddenSchemas=[]
  for Lane in Config["lanes"]:
    if Lane!=DestinLane:
      ForbiddenSchemas.extend(Config["lanes"][Lane]["used_schemas"])

  #Do source code lane copy
  Status,Message=ProjectLaneReplicate(RunMode,GitRepoPath,SourceLane,DestinLane,FilePattern,DestinUpdate,DiffBranch,Config,ForbiddenSchemas,IgnoreSchemaCheck)
  if Status==False:
    _pr.Print(Message)
    return False
  
  #Final message
  _pr.Print("Source code lane copy finish successfully")

  #Return success
  return True

# ---------------------------------------------------------------------------------------------------------------------
# Python macro run mode
# ---------------------------------------------------------------------------------------------------------------------
def RunModePythonMacro(MacroName,MacroSpec,MacrosConfig):
  
  #Complete macro replacements
  PythonCode,_=ExpandMacros(MacroSpec,MacrosConfig)
  
  #Execute macro
  try:
    
    #Execute python code
    Context={"__builtins__": __builtins__}
    exec(PythonCode,Context)
  
  #Exception handler
  except Exception as Ex:
    
    #Get exception text
    TraceBack=Ex.__traceback__
    while TraceBack and TraceBack.tb_frame.f_code.co_filename != "<string>":
        TraceBack=TraceBack.tb_next
    ExceptionText="".join(traceback.format_exception(type(Ex),Ex,TraceBack)).strip("\n")
    
    #Report exception
    _pr.Print("Python run macro exception: "+str(Ex))
    _pr.Print(ExceptionText)
    _pr.Print("Passed code:")
    CodePrint(PythonCode)
    return False


  #Return success
  return True

# ---------------------------------------------------------------------------------------------------------------------
# Main process
# ---------------------------------------------------------------------------------------------------------------------

#Help display
if len(sys.argv)<2:
  ShowHelp()
  exit(0)
  
#Get command line arguments
Status,Options=GetCommandLineOptions()
if Status==True:
  RunMode=Options["run_mode"]
  ExecFileName=Options["exec_file_name"]
  ExecFolderName=Options["exec_folder_name"]
  ExecDiffBranch=Options["exec_diff_branch"]
  TestFileName=Options["test_file_name"]
  TestFolderName=Options["test_folder_name"]
  TestDiffBranch=Options["test_diff_branch"]
  ListSchema=Options["list_schema"]
  DropSchema=Options["drop_schema"]
  SqlQuery=Options["sql_query"]
  ConnectionName=Options["connection_name"]
  ConnectionsFile=Options["connections_file"]
  CsvOutput=Options["csv_output"]
  SilentMode=Options["silent_mode"]
  ForceMode=Options["force_mode"]
  IgnoreHash=Options["ignore_hash"]
  IgnoreSchemaCheck=Options["ignore_schema_check"]
  PayloadMode=Options["payload_mode"]
  DisplayTypes=Options["display_types"]
  CombineResults=Options["combine_results"]
  ShowMode=Options["show_mode"]
  DebugMode=Options["debug_mode"]
  ReplDiffBranch=Options["repl_diff_branch"]
  FilePattern=Options["file_pattern"]
  SourceLane=Options["source_lane"]
  DestinLane=Options["destin_lane"]
  DestinUpdate=Options["destin_update"]
  MacroSpec=Options["macro_spec"]
  MacroFilter=Options["macro_filter"]
else:
  exit(1)

#Set filent mode
_pr=PrintingLibrary()
_pr.SetSilentMode(SilentMode)

#Load config files
ConfigFilePath=(os.environ[CFG_ENV_VAR_NAME] if CFG_ENV_VAR_NAME in os.environ else os.path.dirname(AbsPath(sys.argv[0]))+os.sep+CFG_FILE_NAME)
Status,Message,Config=JsonFileParser(ConfigFilePath)
if Status==False:
  _pr.Print(Message)
  exit(1)
MacrosConfig=None
ScLanesConfig=None
if "macros_file" in Config:
  Status,Message,MacrosConfig=JsonFileParser(Config["macros_file"])
  if Status==False:
    _pr.Print(Message)
    exit(1)
if "sclanes_file" in Config:
  Status,Message,ScLanesConfig=JsonFileParser(Config["sclanes_file"])
  if Status==False:
    _pr.Print(Message)
    exit(1)

#If run mode is macro run we need to check passed macro
PreloadLibraries=True
if RunMode=="MACRO-RUN":
  MacroName=MacroSpec.split("(")[0]
  if not MacroName in MacrosConfig:
    _pr.Print(f"Macro {MacroName}() is not defined in macro definition file!")
    exit(1)
  MacroDef=MacrosConfig[MacroName]
  if MacroDef["kind"]=="sql" and len(ConnectionName)==0:
    _pr.Print(f"Macro {MacroName}() is defined as SQL and connection is not speficied, use --con option!")
    exit(1)
  if MacroDef["kind"]=="python" and len(ConnectionName)!=0:
    _pr.Print(f"Macro {MacroName}() is defined as python and connection parameter must not be provided!")
    exit(1)
  if MacroDef["kind"]=="python":
    PreloadLibraries=False

#Preload snowflake libraries
if PreloadLibraries==True:
  PreloadSnowflakeLibraries()

#Check connections exist on config file and check connection readiness
if RunMode in ["EXEC-FILE","EXEC-FOLDER","EXEC-CHANGES","EXEC-DIFF","TEST-FILE","TEST-FOLDER","TEST-CHANGES","TEST-DIFF","SCHEMA-LIST","SCHEMA-CLEAN","EXEC-SQL"] or (RunMode=="MACRO-RUN" and MacroDef["kind"]=="sql"):
  for Connection in ConnectionName.split(","):
    if Connection not in Config["connections"]:
      _pr.Print(f"Connection {Connection} is not on the configuration file!")
      exit(1)
    if "environment" not in Config["connections"][Connection] or len(Config["connections"][Connection]["environment"])==0:
      _pr.Print(f"Connection {Connection} dos not have environment assigned on configuration file!")
      exit(1)
  if Config["check_connection"]==True:
    Status=CheckConnectionReadiness(Config)
    if Status==False:
      exit(1)

#Script execution modes
if RunMode in ["EXEC-FILE","EXEC-FOLDER","EXEC-CHANGES","EXEC-DIFF"]:
  Status=RunModeScriptExecution(RunMode,ExecFileName,ExecFolderName,ExecDiffBranch,ConnectionName,ConnectionsFile,ForceMode,IgnoreHash,ShowMode,DebugMode,IgnoreSchemaCheck,Config)

#Script testing modes
elif RunMode in ["TEST-FILE","TEST-FOLDER","TEST-CHANGES","TEST-DIFF"]:
  Status=RunModeScriptExecution(RunMode,TestFileName,TestFolderName,TestDiffBranch,ConnectionName,ConnectionsFile,ForceMode,IgnoreHash,ShowMode,DebugMode,IgnoreSchemaCheck,Config,ScLanesConfig,TestMode=True)

#Execute single SQL Query mode
elif RunMode=="EXEC-SQL":
  Status=RunModeSqlQuery(ConnectionName,ConnectionsFile,SqlQuery,DisplayTypes,CombineResults,CsvOutput,Config,MacrosConfig,ShowMode,DebugMode,PayloadMode)

#Execute single SQL Query from macro
elif RunMode=="MACRO-RUN" and MacroDef["kind"]=="sql":
  Status=RunModeSqlQuery(ConnectionName,ConnectionsFile,MacroSpec,DisplayTypes,CombineResults,CsvOutput,Config,MacrosConfig,ShowMode,DebugMode,PayloadMode)

#Execute python code from macro
elif RunMode=="MACRO-RUN" and MacroDef["kind"]=="python":
  Status=RunModePythonMacro(MacroName,MacroSpec,MacrosConfig)

#List objects in schema
elif RunMode=="SCHEMA-LIST":
  Status=RunModeListSchema(ListSchema,ConnectionName,ConnectionsFile,Config)

#Drop objects in schema
elif RunMode=="SCHEMA-CLEAN":
  Status=RunModeDropSchema(DropSchema,ConnectionName,ConnectionsFile,Config)

#Project code replication modes
elif RunMode in ["REPL-FULL","REPL-CHANGES","REPL-DIFF","REPL-FILE"]:
  Status=RunModeProjectReplication(RunMode,SourceLane,DestinLane,FilePattern,DestinUpdate,ReplDiffBranch,IgnoreSchemaCheck,ScLanesConfig)

#Display macros
elif RunMode in ["MACRO-LIST","MACRO-DETAIL"]:
  Status=RunModeMacros(RunMode,MacroFilter,MacrosConfig)

#Return code
if Status==False:
  exit(1)
else:
  exit(0)