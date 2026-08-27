import typer
from pathlib import Path
from rich.table import Table
from typing import Optional
import duckdb
import os

from warehouse_to_go.utils.config import Config
from warehouse_to_go.utils.output import (
    print_info,
    print_success,
    print_status,
    print_error,
)
from warehouse_to_go.extractor.manifest_parser import ManifestParser
from warehouse_to_go.warehouse import (
    get_adapter_factory,
    Identifier,
)
from warehouse_to_go.sink import setup, load as load_into_duckdb

app = typer.Typer(
    name="warehouse-to-go",
    help="Tool to create local DuckDB representations of data warehouse sources from dbt projects.",
    add_completion=True,
    rich_markup_mode=None,
)


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
def debug():
    """Initialize the configuration and test connections."""
    try:
        config = get_config(
            app.config_path,
            app.profile,
            app.target,
            app.manifest_path,
        )

        # Test the selected warehouse connection
        print_status("Testing warehouse connection...")
        adapter = get_adapter_factory(config.warehouse.type)(config)
        try:
            adapter.test_connection(config)
            print_success(f"{config.warehouse.type} connection successful!")
        finally:
            adapter.close()

        # Test DuckDB database creation
        print_status("Testing DuckDB database creation...")
        duckdb.connect(str(config.duckdb.database_path)).close()
        print_success("DuckDB database creation successful!")

        print_success("Configuration initialized successfully!")

    except KeyError as e:
        print_error(f"No adapter available for warehouse type: {e}")
        raise typer.Exit(1)
    except Exception as e:
        print_error(f"Error initializing configuration: {str(e)}")
        raise typer.Exit(1)

@app.command()
def analyze():
    """Analyze the manifest file and show source summary."""
    try:
        config = get_config(
            app.config_path,
            app.profile,
            app.target,
            app.manifest_path,
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
        
        print_info(table)

    except Exception as e:
        print_error(f"Error analyzing manifest: {str(e)}")
        raise typer.Exit(1)

@app.command()
def extract(
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
    """Extract data from the configured warehouse into DuckDB."""
    try:
        # Load config
        config = get_config(
            app.config_path,
            app.profile,
            app.target,
            app.manifest_path,
        )
        
        # Parse manifest
        parser = ManifestParser(config.manifest_path)
        sources = parser.parse_manifest()
        
        # Filter sources if requested
        if source_filter:
            filtered_sources = {k: v for k, v in sources.items() if k == source_filter}
            if not filtered_sources:
                print_error(f"No sources found matching filter: {source_filter}")
                raise typer.Exit(1)
            sources = filtered_sources
        
        # Get extraction plan
        plan = parser.get_extraction_plan(sources)
        
        if dry_run:
            print_info("\n📋 Extraction Plan (Dry Run):", style="bold cyan")
            for db_schema, tables in plan.items():
                print_info(f"\n{db_schema}", style="cyan")
                for table in tables:
                    print_info(
                        f"  • {table['table_name']} "
                        f"(max {config.extract.row_limit:,} rows)",
                        style="dim",
                    )
            return

        # Extract data through the adapter + sink
        adapter = get_adapter_factory(config.warehouse.type)(config)
        total = 0
        con = None
        try:
            adapter.connect(config)
            print_info("\n🚀 Starting extraction...", style="bold cyan")
            layout = adapter.build_layout(config, plan)
            # Hold one DuckDB connection open for the whole extraction: a single
            # ATTACH of the sibling files + one schema-creation pass, then write
            # every fetched table into it before closing.
            con = setup(layout)
            for db_schema, table_list in plan.items():
                for table in table_list:
                    db, _, schema = db_schema.partition(".")
                    # Delegate query construction to the adapter: hand it the
                    # structured database/schema/table identity and let it build
                    # the dialect-specific SELECT it can actually execute, rather
                    # than a Snowflake-flavoured query string.
                    identifier = Identifier(
                        database=db, schema=schema, table=table["identifier"],
                    )
                    with print_status(f"Extracting {identifier.qualified()}..."):
                        fetched = adapter.fetch(
                            identifier,
                            columns=table.get("columns"),
                            limit=config.extract.row_limit,
                        )
                    n = load_into_duckdb(layout, fetched, connection=con)
                    total += n
                    print_success(f"✓ {identifier.qualified()}: {n:,} rows")
            adapter.close()
            source_tables = sum(len(t) for t in plan.values())
            print_success(
                f"\nExtracted {source_tables:,} tables, {total:,} rows"
            )
        except Exception as e:
            print_error(f"Error during extraction: {str(e)}")
            adapter.close()
            if con is not None:
                con.close()
            raise typer.Exit(1)
    except typer.Exit:
        raise
    except KeyError as e:
        print_error(f"No adapter available for warehouse type: {e}")
        raise typer.Exit(1)
    except Exception as e:
        print_error(f"Error during extraction: {str(e)}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
