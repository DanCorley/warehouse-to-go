import typer
from pathlib import Path
from rich.console import Console
from rich.table import Table
from typing import Optional
import duckdb
import os

from warehouse_to_go.utils.config import Config
from warehouse_to_go.extractor.manifest_parser import ManifestParser
from warehouse_to_go.extractor.snowflake_extractor import SnowflakeExtractor, test_connection

app = typer.Typer(
    name="warehouse-to-go",
    help="Tool to create local DuckDB representations of data warehouse sources from dbt projects.",
    add_completion=False,
)
console = Console()

def get_config(
    config_path: Optional[Path] = None,
    profile: Optional[str] = None,
    target: Optional[str] = None,
    manifest_path: Optional[Path] = None,
) -> Config:
    """
    Helper function to get config with optional overrides.
    
    Args:
        config_path: Optional path to a config file
        profile: Optional dbt profile to use
        target: Optional dbt target to use
        manifest_path: Path to dbt manifest file
        
    Returns:
        Config object with the specified settings
    """
    # First try to load from config.yml if it exists
    default_config_path = Path("config.yml")
    if not config_path and default_config_path.exists():
        config_path = default_config_path
    
    if config_path:
        import yaml
        with open(config_path) as f:
            config_dict = yaml.safe_load(f)
            config = Config.from_dict(config_dict)
    else:
        config = Config.from_env()
    
    if profile or target:
        config.warehouse = config.warehouse.from_dbt_profile(
            profile_name=profile,
            target=target
        )
    
    if manifest_path:
        config.manifest_path = Path(manifest_path)
    
    return config

@app.callback()
def main(
    config_path: str = typer.Option(
        None,
        "--config",
        "-c",
        help="Optional path to config file. If not provided, will look for config.yml in the current directory, then fall back to defaults.",
    ),
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        "-p",
        help="Optional dbt profile to use. If not provided, will use the first warehouse profile found.",
    ),
    target: Optional[str] = typer.Option(
        None,
        "--target",
        "-t",
        help="Optional dbt target to use. If not provided, will use the profile's default target.",
    ),
    manifest_path: Path = typer.Option(
        "target/manifest.json",
        "--manifest",
        "-m",
        help="Path to dbt manifest.json file",
    ),
):
    """Tool to create local DuckDB representations of Snowflake sources from dbt projects."""
    # Store the options in the app object
    app.config_path = config_path
    app.profile = profile
    app.target = target
    app.manifest_path = manifest_path

@app.command()
def debug(ctx: typer.Context):
    """Initialize the configuration and test connections."""
    try:
        config = get_config(
            ctx.parent.params["config_path"],
            ctx.parent.params["profile"],
            ctx.parent.params["target"],
            ctx.parent.params["manifest_path"]
        )
            
        # Test Snowflake connection
        console.print("Testing Snowflake connection...", style="yellow")
        test_connection(config)
        console.print("✅ Snowflake connection successful!", style="green")
        
        # Test DuckDB creation
        console.print("Testing DuckDB database creation...", style="yellow")
        conn = duckdb.connect(os.path.join('databases', str(config.duckdb.database_path)))
        conn.close()
        console.print("✅ DuckDB database creation successful!", style="green")
        
        console.print("✅ Configuration initialized successfully!", style="green")
        
    except Exception as e:
        console.print(f"❌ Error initializing configuration: {str(e)}", style="red")
        raise typer.Exit(1)

@app.command()
def analyze(ctx: typer.Context):
    """Analyze the manifest file and show source summary."""
    try:
        config = get_config(
            ctx.parent.params["config_path"],
            ctx.parent.params["profile"],
            ctx.parent.params["target"],
            ctx.parent.params["manifest_path"]
        )
        parser = ManifestParser(config.manifest_path)
        sources = parser.parse_manifest()
        
        # Create summary table
        table = Table(title="Source Summary")
        table.add_column("Source", style="cyan")
        table.add_column("Database", style="green")
        table.add_column("Schema", style="yellow")
        table.add_column("Tables", justify="right", style="magenta")
        
        for source_name, config in sources.items():
            table.add_row(
                source_name,
                config.database,
                config.schema,
                str(len(config.tables))
            )
        
        console.print(table)
        
    except Exception as e:
        console.print(f"❌ Error analyzing manifest: {str(e)}", style="red")
        raise typer.Exit(1)

@app.command()
def extract(
    ctx: typer.Context,
    source_filter: Optional[str] = typer.Option(
        None,
        "--source",
        "-s",
        help="Filter to specific source name",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show what would be extracted without actually extracting",
    ),
):
    """Extract data from Snowflake to DuckDB."""
    try:
        # Load config
        config = get_config(
            ctx.parent.params["config_path"],
            ctx.parent.params["profile"],
            ctx.parent.params["target"],
            ctx.parent.params["manifest_path"]
        )
        
        # Parse manifest
        parser = ManifestParser(config.manifest_path)
        sources = parser.parse_manifest()
        
        # Filter sources if requested
        if source_filter:
            filtered_sources = {k: v for k, v in sources.items() if k == source_filter}
            if not filtered_sources:
                console.print(f"❌ No sources found matching filter: {source_filter}", style="red")
                raise typer.Exit(1)
            sources = filtered_sources
        
        # Get extraction plan
        plan = {}
        for source_name, source_config in sources.items():
            key = f"{source_config.database}.{source_config.schema}"
            if key not in plan:
                plan[key] = []
            
            for table in source_config.tables:
                plan[key].append({
                    'source_name': source_name,
                    'table_name': table.name,
                    'identifier': table.identifier,
                    'columns': table.columns,
                    'meta': {**source_config.meta, **table.meta} if table.meta else source_config.meta
                })
        
        if dry_run:
            console.print("\n📋 Extraction Plan (Dry Run):", style="bold cyan")
            for db_schema, tables in plan.items():
                console.print(f"\n[cyan]{db_schema}[/cyan]")
                for table in tables:
                    console.print(
                        f"  • {table['table_name']} "
                        f"[dim](max {config.extract.row_limit:,} rows, "
                        f"{config.extract.batch_size:,} per batch)[/dim]"
                    )
            return

        # Extract data
        console.print("\n🚀 Starting extraction...", style="bold cyan")
        with SnowflakeExtractor(config) as extractor:
            extractor.extract_tables(plan)
    except Exception as e:
        console.print(f"[red]✗[/red] Error during extraction: {str(e)}", style="red")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
