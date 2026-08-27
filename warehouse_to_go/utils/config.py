from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional
import yaml


@dataclass
class WarehouseConfig:
    """A *generic* warehouse profile — an adapter-agnostic view of a dbt output.

    This is deliberately **not** a connection model. It carries the output's
    declared ``type`` plus the raw remainder of the output (``raw``) untouched,
    so each adapter — parsed behind its selected factory — extracts exactly the
    fields it understands and ignores everything else. There is no
    Snowflake-shaped connection here: no ``account``/``warehouse``/``schema``/
    ``password`` attributes and no password/private-key auth enforcement. Those
    are adapter concerns, resolved where the factory's adapter parses its own
    profile.

    ``type`` is the only field the framework inspects (validated against the
    adapter registry); every other field is provider-specific.
    """

    type: str
    raw: Dict[str, Any] = field(default_factory=dict)

    # Dialect-agnostic knobs some adapters may read. Anything unrecognised lives
    # in ``raw`` and is ignored by adapters that don't care about it.
    threads: int = 4
    client_session_keep_alive: bool = False
    query_tag: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        """Return the adapter-relevant fields as a plain dict (``raw`` included)."""
        data: Dict[str, Any] = {"type": self.type, **dict(self.raw)}
        for name in ("threads", "client_session_keep_alive", "query_tag"):
            value = getattr(self, name)
            if value is not None:
                data[name] = value
        return data

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
        from warehouse_to_go.warehouse import adapter_registry
        if not profile_name:
            for name, config in profiles.items():
                if isinstance(config, dict) and 'outputs' in config:
                    for output_name, output_config in config['outputs'].items():
                        if output_config.get("type") not in adapter_registry():
                             continue
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
        # The output's `type` is the adapter selector — it drives the registry
        adapter_type = config.get("type")
        if not adapter_type:
            raise ValueError(
                f"Target {target} in profile {profile_name} must declare a warehouse 'type'"
            )

        # Validate the selector against the registry *before* it reaches the
        # dispatch lookup, so an unregistered adapter fails fast with a clear
        # message instead of silently resolving to a different (or unknown) one.
        from warehouse_to_go.warehouse import adapter_registry
        if adapter_type not in adapter_registry():
            registered = ", ".join(sorted(adapter_registry())) or "(none)"
            raise ValueError(
                f"No adapter registered for warehouse type {adapter_type!r}. "
                f"Registered adapters: {registered}"
            )

        config = profile['outputs'][target]
        # The output's `type` is the adapter selector — it drives the registry
        adapter_type = config.get("type")
        if not adapter_type:
            raise ValueError(
                f"Target {target} in profile {profile_name} must declare a warehouse 'type'"
            )

        # Validate the selector against the registry *before* it reaches the
        # dispatch lookup, so an unregistered adapter fails fast with a clear
        # message instead of silently resolving to a different (or unknown) one.
        from warehouse_to_go.warehouse import adapter_registry
        if adapter_type not in adapter_registry():
            registered = ", ".join(sorted(adapter_registry())) or "(none)"
            raise ValueError(
                f"No adapter registered for warehouse type {adapter_type!r}. "
                f"Registered adapters: {registered}"
            )

        # Every other field in the output is provider-specific. Keep it verbatim
        # in `raw` (excluding dbt-only keys) so each adapter — behind its
        # factory — parses the fields it recognises and ignores the rest. No
        # hard-coded Snowflake fields or auth rules are imposed here, otherwise a
        # Postgres/BigQuery profile would be forced to look Snowflake-shaped and
        # the "add one adapter file" contract would break.
        raw = {k: v for k, v in config.items() if k not in ("type", "outputs", "target")}
        return cls(
            type=adapter_type,
            raw=raw,
            threads=raw.get("threads", 4),
            client_session_keep_alive=raw.get("client_session_keep_alive", False),
            query_tag=raw.get("query_tag"),
        )

@dataclass
class DuckDBConfig:
    """Configuration for DuckDB connection."""
    database_path: Path

@dataclass
class ExtractConfig:
    """Configuration for data extraction settings."""
    row_limit: int = 10000  # Default limit of rows per table

@dataclass
class Config:
    """Main configuration class for the application."""
    warehouse: WarehouseConfig
    duckdb: DuckDBConfig
    extract: ExtractConfig = field(default_factory=ExtractConfig)
    manifest_path: Path = field(default_factory=lambda: Path("target/manifest.json"))
    
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
                database_path=Path("warehouse_mirror.duckdb"),
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
            ),
            manifest_path=manifest_path,
        )
