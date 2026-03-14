# Snowflake Development Toolkit

Command-line toolkit for managing Snowflake development workflows. Allows executing SQL scripts, testing changes in isolated schemas, performing schema operations, running SQL queries, working with macros, and replicating code across development lanes.

Integrates with **Git** to detect changed files and tracks execution history to avoid re-running scripts that have already been deployed.

Key features:

- Execute SQL scripts from file, folder, or git-detected changes
- Test-mode execution using isolated schemas via source code lane replication
- Schema listing and cleanup utilities
- Code replication across source code lanes (e.g. core → test)
- SQL execution with optional CSV output and multi-connection support
- Macro system for reusable SQL and Python logic
- Jinja2 template support for environment-aware scripts
- Network/VPN readiness check before connecting

---

## Installation

### Requirements

- Python 3.7+
- Git
- Access to a Snowflake account

### Python dependencies

Install the following Python packages:

```
pip install snowflake-connector-python jinja2 chardet
```

The tool also depends on the companion library `printlib.py` (from the **PythonLibs** repository). Ensure it is accessible in Python's module path (e.g. by placing it alongside `sf.py` or adding its directory to `PYTHONPATH`).

### Setup

1. Clone the repository and place `sf.py` in a directory of your choice.

2. Shell wrappers `sf.bat` (Windows) and `sf` (Linux/macOS) are provided for convenience so you can invoke the tool simply as:

   ```
   sf <options>
   ```

   Add the tool directory to your system `PATH` to make it available from anywhere.

3. Set the environment variable `SF_CONFIG_PATH` to point to the directory containing the main configuration file `sf-cfg.json`:

   ```
   set SF_CONFIG_PATH=C:\path\to\config\directory      # Windows
   export SF_CONFIG_PATH=/path/to/config/directory      # Linux / macOS
   ```

   If this variable is not set, the tool looks for `sf-cfg.json` in the same directory as `sf.py`.

4. Set the Snowflake connections file path. The tool reads the standard Snowflake `connections.toml` file. Provide its location through either:

   - The environment variable `SNOWFLAKE_CONN`:
     ```
     set SNOWFLAKE_CONN=C:\path\to\connections.toml        # Windows
     export SNOWFLAKE_CONN=/path/to/connections.toml        # Linux / macOS
     ```
   - Or the `--sfcon` command-line option on each invocation.

---

## Configuration

The tool uses three JSON configuration files. All files support `//` line comments.

### 1. Main configuration — `sf-cfg.json`

This is the primary configuration file. It contains:

| Section | Description |
|---|---|
| `check_connection` | Boolean. When `true`, the tool checks network/VPN readiness before connecting to Snowflake. |
| `intranet_wlans` | List of WLAN names considered as intranet (no VPN needed). |
| `remote_wlans` | List of WLAN names considered as remote (VPN required). |
| `connected_wlan_command` | OS command to detect connected WLAN. |
| `network_interfaces_command` | OS command to list network interfaces (used to detect VPN). |
| `vpn_interface_name` | Name of the VPN network interface to look for. |
| `retry_sleep_seconds` | Seconds to wait between connection readiness retries. |
| `retry_times` | Number of retries for connection readiness check. |
| `macros_file` | Absolute path to the macros configuration file (`sf-macros.json`). |
| `sclanes_file` | Absolute path to the source code lanes configuration file (`sf-sclanes.json`). |
| `jinja2_templates_def` | Named Jinja2 template definitions. Each template is a list of variable/value pairs used to render SQL scripts before execution. Values prefixed with `@` are resolved from the connection's properties (e.g. `@environment`). |
| `execution_rules_def` | Named sets of regex-based rules that determine how each SQL statement is handled (`EXECUTE` or `IGNORE`). Rules are evaluated in order; the first match wins. |
| `connections` | Dictionary of named connection profiles. Each connection specifies: |

**Connection properties:**

| Property | Description |
|---|---|
| `environment` | Environment tag (e.g. `DEV`, `INT`, `PRO`) used for Jinja2 template substitution. |
| `jinja2_templates` | List of template names (from `jinja2_templates_def`) to apply to queries. |
| `execution_rules` | List of rule set names (from `execution_rules_def`) for statement classification. |

Example structure:

```json
{
  "check_connection": true,
  "intranet_wlans": ["CORP-WIFI"],
  "remote_wlans": ["HOME-WIFI"],
  "connected_wlan_command": "netsh wlan show interfaces",
  "network_interfaces_command": "ipconfig /all",
  "vpn_interface_name": "VPN Adapter Name",
  "retry_sleep_seconds": 15,
  "retry_times": 20,

  "macros_file": "C:\\path\\to\\sf-macros.json",
  "sclanes_file": "C:\\path\\to\\sf-sclanes.json",

  "jinja2_templates_def": {
    "standard_template": [
      {"variable": "env",         "value": "@environment"},
      {"variable": "environ",     "value": "@environment"},
      {"variable": "environment", "value": "@environment"}
    ]
  },

  "execution_rules_def": {
    "generic_project": [
      {"regex": "^USE\\s+.+$",  "exec_mode": "IGNORE"},
      {"regex": "^.+$",         "exec_mode": "EXECUTE"}
    ]
  },

  "connections": {
    "DEV": {
      "environment": "DEV",
      "jinja2_templates": ["standard_template"],
      "execution_rules": ["generic_project"]
    },
    "INT": {
      "environment": "INT",
      "jinja2_templates": ["standard_template"],
      "execution_rules": ["generic_project"]
    },
    "PRO": {
      "environment": "PRO",
      "jinja2_templates": ["standard_template"],
      "execution_rules": ["generic_project"]
    }
  }
}
```

### 2. Macros configuration — `sf-macros.json`

Defines reusable macros (SQL or Python) that can be invoked from the command line or embedded in `--sql` queries.

Each macro entry has:

| Property | Description |
|---|---|
| `description` | Human-readable description of the macro. |
| `kind` | `"sql"` or `"python"`. SQL macros are executed as Snowflake queries. Python macros are executed locally. |
| `args` | List of argument descriptions. Arguments are referenced as `$1`, `$2`, etc. in the definition. |
| `column_formats` | *(SQL macros only, optional)* List of column display format codes for output formatting. |
| `mdef` | The macro definition body. For SQL macros, this is a SQL query template. For Python macros, this is Python code. Placeholders `$1`, `$2`, ... are replaced with the provided arguments. |

Example:

```json
{
  "my_macro": {
    "description": "Get row count for a table",
    "kind": "sql",
    "args": ["Fully qualified table name"],
    "column_formats": ["R"],
    "mdef": "SELECT COUNT(*) AS ROW_COUNT FROM $1"
  },
  "my_python_macro": {
    "description": "Print a greeting",
    "kind": "python",
    "args": ["Name to greet"],
    "mdef": "print('Hello, $1!')"
  }
}
```

### 3. Source code lanes configuration — `sf-sclanes.json`

Defines project-level source code lane replication rules. This file is a dictionary keyed by the **absolute path of the git repository root**. Each project entry contains:

| Property | Description |
|---|---|
| `lanes` | Dictionary of lane names (e.g. `core`, `test`). Each lane has: `path` (relative path from repo root), `file_suffix` (suffix appended/removed on file names during replication), `modify_warning` (`1` to prompt before modifying), `testing_lane` (name of lane used for test mode), `used_schemas` (list of schemas this lane uses — used for forbidden schema checks). |
| `folders` | List of folder paths (relative to the lane root) that are included in replication. |
| `tags` | List of search tags with their replacement variants (used for environment-aware string substitution). |
| `rules` | List of translation rules. Each rule has a `name` and a string value for each lane name. During replication, the source lane string is replaced with the destination lane string in all replicated files. |

Example:

```json
{
  "C:\\repos\\my-project": {

    "lanes": {
      "core": {
        "path": ".",
        "file_suffix": "",
        "modify_warning": 1,
        "testing_lane": "test",
        "used_schemas": ["MY_SCHEMA"]
      },
      "test": {
        "path": ".test",
        "file_suffix": "TEST",
        "modify_warning": 0,
        "testing_lane": "",
        "used_schemas": ["TEST"]
      }
    },

    "folders": ["tables", "views", "functions", "procedures", "tasks"],

    "tags": [
      {"name": "${tag}", "replacements": ["{{env}}", "<env>"]}
    ],

    "rules": [
      {"name": "use_schema", "core": "USE SCHEMA MY_SCHEMA;",              "test": "USE SCHEMA TEST;"},
      {"name": "objects",    "core": "MY_DB_${tag}.MY_SCHEMA.",            "test": "MY_DB_${tag}.TEST."}
    ]
  }
}
```

---

## Usage

### General syntax

```
sf <run-mode> [options]
```

or directly:

```
python sf.py <run-mode> [options]
```

Running with no arguments displays the built-in help.

---

### Execution modes

The tool requires exactly one run mode per invocation. Run modes are grouped into six categories:

#### 1. Script Execution

Execute SQL script files against a Snowflake connection. Scripts are tracked by modification date and file hash — already-executed scripts are skipped unless `--ignore-hash` is used.

| Mode | Description |
|---|---|
| `--exec-file:<path>` | Execute a single `.sql` script file. |
| `--exec-folder:<path>` | Execute all `.sql` scripts inside a folder (recursively). Scripts are sorted alphabetically before execution. |
| `--exec-changes` | Execute all changed/added `.sql` scripts detected by `git status`. |
| `--exec-diff:<branch>` | Execute all different `.sql` scripts from a `git diff` comparison against `<branch>`. |

Required options: `--con:<name>`

```
sf --exec-file:deploy/setup.sql --con:DEV
sf --exec-folder:scripts/ --con:DEV --force
sf --exec-changes --con:DEV
sf --exec-diff:main --con:INT --ignore-hash
```

#### 2. Script Testing

Same as script execution, but scripts are first replicated into an isolated testing schema (via source code lane configuration) before execution. This allows testing changes without affecting the main schema.

| Mode | Description |
|---|---|
| `--test-file:<path>` | Test a single `.sql` script file. |
| `--test-folder:<path>` | Test all `.sql` scripts inside a folder (recursively). |
| `--test-changes` | Test all changed/added `.sql` scripts detected by `git status`. |
| `--test-diff:<branch>` | Test all different `.sql` scripts from a `git diff` comparison against `<branch>`. |

Required options: `--con:<name>`

Requires source code lanes to be configured in `sf-sclanes.json` for the current git repository.

```
sf --test-file:procedures/my_proc.sql --con:DEV
sf --test-folder:procedures/ --con:DEV --force
sf --test-changes --con:DEV
sf --test-diff:main --con:DEV --ignore-schema-check
```

#### 3. Schema Operations

Inspect or clean up Snowflake schemas.

| Mode | Description |
|---|---|
| `--schema-list:<schema>` | List all objects (tables, views, procedures, functions, tasks) in the given schema. Schema can be just the name (uses current database from connection) or fully qualified as `DATABASE.SCHEMA`. |
| `--schema-drop:<schema>` | Drop all objects in the given schema (but not the schema itself). Prompts for confirmation before executing. |

Required options: `--con:<name>`

```
sf --schema-list:MY_SCHEMA --con:DEV
sf --schema-list:MY_DATABASE.MY_SCHEMA --con:DEV
sf --schema-drop:TEST --con:DEV
```

#### 4. SQL Query Execution

Execute an inline SQL statement directly from the command line. Supports macro expansion, Jinja2 template rendering, and multi-connection execution.

> **Note:** The `--sql` mode does not support `--force` or `--ignore-hash` options.

| Mode | Description |
|---|---|
| `--sql:<query>` | Execute the given SQL statement. The query can contain macro calls (e.g. `my_macro(arg1:arg2)`). |

Required options: `--con:<name>` (multiple connections allowed, separated by comma)

```
sf --sql:"SELECT CURRENT_TIMESTAMP()" --con:DEV
sf --sql:"SELECT COUNT(*) FROM MY_TABLE" --con:DEV --csv
sf --sql:"SELECT * FROM MY_TABLE" --con:DEV,INT --sep
sf --sql:"SELECT * FROM MY_TABLE" --con:DEV --types
```

#### 5. Code Replication

Replicate source code files between project lanes (e.g. from `core` to `test` or vice versa). During replication, files are copied, renamed according to lane suffixes, and translation rules are applied to transform schema and object references.

| Mode | Description |
|---|---|
| `--repl-full` | Full replication. Wipes all files in the destination lane folders before copying from source. |
| `--repl-changes` | Replicate only files detected as changed by `git status`. |
| `--repl-diff:<branch>` | Replicate only files detected as different by `git diff` against `<branch>`. |
| `--repl-file:<pattern>` | Replicate only files matching the given pattern (supports `*` and `?` wildcards). |

Required options: `--lanes:<source>,<destination>`

Must be run from the top-level directory of a git repository that is configured in `sf-sclanes.json`.

```
sf --repl-full --lanes:test,core --update
sf --repl-changes --lanes:core,test --update
sf --repl-diff:main --lanes:test,core --update
sf --repl-file:procedures/*.sql --lanes:test,core --update
```

#### 6. Macros

Browse and execute macros defined in `sf-macros.json`.

| Mode | Description |
|---|---|
| `--macro-list` | Display a summary table of all defined macros with their descriptions, kind, and arguments. |
| `--macro-detail:<pattern>` | Display full details (definition body, arguments) of macros matching the pattern. Supports `*` and `?` wildcards. |
| `--<macro>(<args>)` | Execute a macro. Arguments are separated by `:`. SQL macros require `--con`, Python macros must not use `--con`. |

```
sf --macro-list
sf --macro-detail:*log*
sf --my_macro(arg1:arg2) --con:DEV
sf --my_python_macro(hello)
```

---

### Options reference

| Option | Applies to | Description |
|---|---|---|
| `--con:<name>` | Script execution, testing, schema, SQL, SQL macros | Connection profile name (as defined in `sf-cfg.json`). For `--sql` mode, multiple connections can be specified separated by comma. |
| `--sfcon:<path>` | Script execution, testing, schema, SQL | Path to the Snowflake `connections.toml` file. Overrides the `SNOWFLAKE_CONN` environment variable. |
| `--lanes:<src>,<dst>` | Code replication | Source and destination lane names for code replication. |
| `--update` | Code replication | Enable actual modification of files in the destination lane during replication. |
| `--force` | Script execution, testing | Skip the confirmation prompt before executing scripts. |
| `--ignore-hash` | Script execution, testing | Ignore file hash when checking if a script has already been executed. Forces re-execution of scripts even if they haven't changed. |
| `--ignore-schema-check` | Testing, code replication | Skip the forbidden schema check that validates replicated files don't reference schemas from other lanes. |
| `--types` | SQL query | Display output column metadata (name, type, size, precision, nullability) instead of query results. |
| `--sep` | SQL query | When using multiple connections, display results separately instead of combining them into one table. |
| `--csv` | SQL query | Output results in CSV format. Automatically enables `--silent`. |
| `--show` | Script execution, testing, SQL query | Display the SQL statements that would be executed without actually executing anything (dry-run mode). |
| `--debug` | Script execution, testing, SQL query | Display each SQL query before execution and prompt for confirmation. |
| `--silent` | All modes | Suppress all informational messages; only errors are printed. |

---

### Notes

- Changed/added files from git are detected using `git status --porcelain=v1 --untracked-files=all`.
- Diff-based modes use `git diff --name-status <branch>..HEAD`.
- Scripts are skipped if their modification date is earlier than the last execution date and their file hash matches the last execution hash. This history is stored in a temporary file (`sf-his.json` in the system temp directory).
- Test mode replicates scripts into an isolated testing lane (as configured in `sf-sclanes.json`) before execution, so the original schema is not affected.
- In `--sql` mode, multiple connection names can be given separated by comma. Results are combined into a single table by default (use `--sep` to keep them separate).
- Macros can be used inside `--sql` queries using the syntax `macro_name(arg1:arg2)`.
- SQL scripts and queries are processed through Jinja2 templates defined in the connection's configuration, allowing environment-specific variable substitution (e.g. `{{env}}`).
- For code replication modes, the tool must be run from the top level of a git repository that is configured in `sf-sclanes.json`.
