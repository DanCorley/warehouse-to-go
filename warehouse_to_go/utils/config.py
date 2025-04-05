from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import yaml

@dataclass
class WarehouseConfig:
    """Configuration for warehouse connection."""
    account: str
    user: str
    warehouse: str
    role: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    # Authentication fields
    private_key_path: Optional[str] = None
    private_key_passphrase: Optional[str] = None
    password: Optional[str] = None
    # Additional settings
    threads: int = 4
    client_session_keep_alive: bool = False
    query_tag: Optional[str] = None

    @classmethod
    def from_dbt_profile(cls, profile_dir: Optional[Path] = None, profile_name: Optional[str] = None, target: Optional[str] = None) -> 'WarehouseConfig':
        """
        Create warehouse config from dbt profiles.yml
        
        Args:
            profile_dir: Directory containing the profiles.yml file
            profile_name: Name of the profile to use
            target: Target environment within the profile
            
        Returns:
            WarehouseConfig object with connection details
        """
        if not profile_dir:
            profile_dir = Path.home() / '.dbt'
        
        profiles_path = profile_dir / 'profiles.yml'
        if not profiles_path.exists():
            raise FileNotFoundError(f"dbt profiles.yml not found at {profiles_path}")

        with open(profiles_path) as f:
            profiles = yaml.safe_load(f)

        # If profile_name not provided, use first profile that has a warehouse connection
        if not profile_name:
            for name, config in profiles.items():
                if isinstance(config, dict) and 'outputs' in config:
                    for output_name, output_config in config['outputs'].items():
                        if output_config.get('type') == 'snowflake':
                            profile_name = name
                            target = target or output_name
                            break
                    if profile_name:
                        break

        if not profile_name:
            raise ValueError("No warehouse profile found in profiles.yml")

        profile = profiles[profile_name]
        if 'outputs' not in profile:
            raise ValueError(f"Profile {profile_name} has no outputs section")

        # Use specified target or profile's default target
        target = target or profile.get('target', 'dev')
        if target not in profile['outputs']:
            raise ValueError(f"Target {target} not found in profile {profile_name}")

        config = profile['outputs'][target]
        if config.get('type') not in ['snowflake']:
            raise ValueError(f"Target {target} in profile {profile_name} is not a supported warehouse connection")

        # Copy all fields from the profile
        warehouse_config = {
            k: v for k, v in config.items() 
            if k not in ['type', 'outputs', 'target']  # Exclude dbt-specific fields
        }

        # Handle authentication fields
        auth_fields = {
            'private_key_path', 'private_key_passphrase', 'password'
        }
        auth_provided = {k: v for k, v in warehouse_config.items() if k in auth_fields}

        if not auth_provided:
            raise ValueError(
                f"No authentication method found in profile {profile_name}. "
                "Expected one of: password, private_key_path"
            )

        return cls(**warehouse_config)

@dataclass
class DuckDBConfig:
    """Configuration for DuckDB connection."""
    database_path: Path

@dataclass
class ExtractConfig:
    """Configuration for data extraction settings."""
    row_limit: int = 10000  # Default limit of rows per table
    batch_size: int = 10000  # Number of rows to fetch at once

@dataclass
class Config:
    """Main configuration class for the application."""
    warehouse: WarehouseConfig
    duckdb: DuckDBConfig
    extract: ExtractConfig = ExtractConfig()
    manifest_path: Path = Path("target/manifest.json")
    
    @classmethod
    def from_env(cls) -> 'Config':
        """
        Create configuration from environment variables.
        
        Returns:
            Config object initialized with values from environment
        """
        return cls(
            warehouse=WarehouseConfig.from_dbt_profile(),
            duckdb=DuckDBConfig(
                database_path=Path("warehouse_mirror.duckdb"),  # Use current directory
            ),
        )
    
    @classmethod
    def from_dict(cls, config_dict: dict) -> 'Config':
        """
        Create configuration from dictionary.
        
        Args:
            config_dict: Dictionary containing configuration values
            
        Returns:
            Config object initialized with values from the dictionary
        """
        warehouse_config = config_dict.get('warehouse', {})
        # Always use dbt profile with optional overrides
        profile_name = warehouse_config.get('profile_name')
        target = warehouse_config.get('target')
        wh_config = WarehouseConfig.from_dbt_profile(
            profile_name=profile_name,
            target=target
        )

        # Ensure manifest_path is a Path object
        manifest_path = config_dict.get('manifest_path', 'target/manifest.json')
        if isinstance(manifest_path, str):
            manifest_path = Path(manifest_path)

        return cls(
            warehouse=wh_config,
            duckdb=DuckDBConfig(
                database_path=Path(config_dict.get("duckdb", {}).get("database_path", "warehouse_mirror.duckdb")),
            ),
            extract=ExtractConfig(
                row_limit=config_dict.get("extract", {}).get("row_limit", 10000),
                batch_size=config_dict.get("extract", {}).get("batch_size", 10000)
            ),
            manifest_path=manifest_path,
        )
