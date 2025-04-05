from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional
import json

@dataclass
class TableConfig:
    """Configuration for a table to extract."""
    name: str
    identifier: str
    columns: Optional[List[str]] = None
    meta: Dict = field(default_factory=dict)

@dataclass
class SourceConfig:
    """Configuration for a source to extract."""
    database: str
    schema: str
    tables: List[TableConfig] = field(default_factory=list)
    meta: Dict = field(default_factory=dict)

class ManifestParser:
    """Parser for dbt manifest files."""
    
    def __init__(self, manifest_path: Path):
        """Initialize with path to manifest file."""
        self.manifest_path = manifest_path
        
    def parse_manifest(self) -> Dict[str, SourceConfig]:
        """
        Parse the manifest file and extract source configurations.
        
        Returns:
            Dictionary mapping source names to their configurations
        """
        if not self.manifest_path.exists():
            raise FileNotFoundError(f"Manifest file not found at {self.manifest_path}")
            
        with open(self.manifest_path) as f:
            manifest = json.load(f)
            
        sources = {}
        
        # Process each source in the manifest
        for node_name, node in manifest.get('sources', {}).items():
            source_name = node.get('source_name')
            if not source_name:
                continue
                
            # Get or create source config
            if source_name not in sources:
                sources[source_name] = SourceConfig(
                    database=node.get('database', ''),
                    schema=node.get('schema', ''),
                    meta=node.get('meta', {})
                )
                
            # Add table config
            table_name = node.get('name', '')
            if table_name:
                table = TableConfig(
                    name=table_name,
                    identifier=node.get('identifier', table_name),
                    columns=node.get('columns'),
                    meta=node.get('meta', {})
                )
                sources[source_name].tables.append(table)
                
        return sources

    def get_extraction_plan(self) -> Dict[str, List[Dict]]:
        """
        Generate a plan for extracting data from Snowflake.
        Returns a dictionary mapping database.schema to list of tables.
        """
        extraction_plan = {}
        
        for source_name, source_config in self.sources.items():
            key = f"{source_config.database}.{source_config.schema}"
            if key not in extraction_plan:
                extraction_plan[key] = []
            
            for table in source_config.tables:
                extraction_plan[key].append({
                    'source_name': source_name,
                    'table_name': table.name,
                    'identifier': table.identifier,
                    'columns': table.columns,
                    'meta': {**source_config.meta, **table.meta} if table.meta else source_config.meta
                })
        
        return extraction_plan

def main():
    # Example usage
    manifest_path = Path('target/manifest.json')
    parser = ManifestParser(manifest_path)
    sources = parser.parse_manifest()
    
    # Print summary
    print("\nSource Summary:")
    for source_name, config in sources.items():
        print(f"\nSource: {source_name}")
        print(f"Database: {config.database}")
        print(f"Schema: {config.schema}")
        print(f"Tables: {len(config.tables)}")

if __name__ == '__main__':
    main()
