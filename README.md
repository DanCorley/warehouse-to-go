# Warehouse to Go

A tool to create local DuckDB representations of data warehouse sources, making it easy to use DuckDB as a replacement in dbt projects.

<div align="center">
  <img src="./terminalizer_render.gif"  style="max-width: 75%;" />
</div>

## ✨ Features

### 🔄 Data Extraction
- **Smart Source Analysis**: Automatically extracts warehouse source tables from your dbt project
- **Row Limit Control**: Configurable row limits for faster development (default: 10,000 rows)
- **Batch Processing**: Efficient data extraction with configurable batch sizes
- **Schema Preservation**: Maintains original database and schema names for seamless integration

### 🔐 Authentication & Configuration
- **dbt Integration**: Seamlessly uses your existing `profiles.yml` for warehouse credentials
- **Flexible Auth**: Supports multiple authentication methods:
  - 🔑 Password authentication
  - 🔐 Key-based authentication (with optional passphrase)
- **Profile Settings**: Preserves all dbt profile configurations:
  - 🏷️ Query tags
  - ⚙️ Session parameters
  - 🔌 Connection settings

### 🚀 DuckDB Integration
- **Automatic Schema Creation**: Creates DuckDB database with matching structure
- **Development Ready**: Perfect for local development and testing

## Installation

<div align="center">
  <p style="font-size: 18px"> Try it out with the sample dbt project -> <a href="./dbt/README.md">warehouse-to-go/dbt</p>
</div>


```bash
# 1. Clone the repository:
git clone https://github.com/dancorley/warehouse-to-go.git
cd warehouse-to-go

# 2. Create and activate a virtual environment (recommended)
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# 3. Install dependencies:
pip install -e .
```

## Configuration

The tool uses your dbt profile configuration from `~/.dbt/profiles.yml` for warehouse credentials. It will automatically use the first warehouse profile it finds, with the default target (usually 'dev').

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

### Configuration Loading Order

The tool loads configuration in the following order:
1. Command line arguments (--profile, --target, etc.)
2. `config.yml` file in the current directory (if it exists)
3. Default configuration


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
