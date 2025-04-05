# Warehouse to Go

A tool to create local DuckDB representations of data warehouse sources, making it easy to use DuckDB as a replacement in dbt projects.

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/warehouse-to-go.git
cd warehouse-to-go
```

2. Create and activate a virtual environment (recommended):
```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

The tool uses your dbt profile configuration from `~/.dbt/profiles.yml` for warehouse credentials. It will automatically use the first warehouse profile it finds, with the default target (usually 'dev').

### Configuration Loading Order

The tool loads configuration in the following order:
1. Command line arguments (--profile, --target, etc.)
2. `config.yml` file in the current directory (if it exists)
3. Default configuration

### Default Configuration

If no configuration is provided, the tool uses these defaults:
- Uses the first warehouse profile found in your dbt profiles
- Uses the profile's default target
- Creates a DuckDB database named `warehouse_mirror.duckdb` in the current directory
- Extracts up to 10,000 rows per table
- Fetches data in batches of 10,000 rows

### Using a Config File

You can create a `config.yml` file in your project directory to customize the tool's behavior:

```yaml
# config.yml
warehouse:
  # Optional: specify which dbt profile to use
  # If not specified, will use the first warehouse profile found
  profile_name: my_profile
  
  # Optional: specify which target to use
  # If not specified, will use the profile's default target
  target: prod

duckdb:
  # Optional: path to the DuckDB database file
  database_path: ./databases

extract:
  # Optional: maximum number of rows to extract per table
  row_limit: 10000
  
  # Optional: number of rows to fetch at once
  batch_size: 10000
```

To use a different config file, specify its path when running commands:
```bash
warehouse-to-go extract --config my_config.yml
```

## Usage

1. Initialize and test connections:
```bash
warehouse-to-go debug
```

2. Analyze your dbt project's sources:
```bash
warehouse-to-go analyze
```

3. Preview data extraction from specific source:
```bash
warehouse-to-go extract --source my_source --dry-run
```

4. Specify a different dbt profile or target:
```bash
warehouse-to-go extract --profile my-project --target dev
```

## Features

- Uses dbt profiles.yml for warehouse credentials
- Supports all warehouse authentication methods:
  - Password authentication
  - Key-based authentication (with optional passphrase)
- Preserves all dbt profile settings:
  - Query tags
  - Session parameters
  - Thread counts
  - Connection settings
- Extracts warehouse source tables defined in dbt project
- Limits row count per table (default: 10,000)
- Processes data in batches to manage memory
- Creates DuckDB database with matching structure
- Preserves database and schema names
- Shows progress with rich progress bars
- Supports source filtering
- Includes dry-run capability

## Development

Install development dependencies:
```bash
pip install -r requirements.txt
```

Run tests:
```bash
pytest
```

Format code:
```bash
black src tests
isort src tests
```

Type checking:
```bash
mypy src
``` 