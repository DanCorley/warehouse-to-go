import os
from rich.console import Console

# markup=False keeps output literal and predictable regardless of the Typer
# rich_markup_mode setting; callers color via the `style` kwarg.
console = Console(markup=False)


def print_info(msg="", **kwargs) -> None:
    """General program output: headers, tables, summaries, plans."""
    console.print(msg, **kwargs)


def print_success(msg: str) -> None:
    """Success confirmation."""
    console.print(f"{msg}", style="green")


def print_status(msg: str):
    """Transient status message (spinner). Returns a context manager."""
    return console.status(msg, spinner="dots")


def print_error(msg: str) -> None:
    """Error message in red."""
    console.print(f"{msg}", style="red")


def print_debug(msg: str) -> None:
    """Verbose diagnostic. Only shown when WH_GOTO_DEBUG is set."""
    if os.environ.get("WH_GOTO_DEBUG"):
        console.print(f"debug: {msg}", style="dim")
