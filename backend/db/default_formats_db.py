import sqlite3
import threading
from core import get_settings, validate_sql_identifier

'''
Anywhere you see # nosec B608, it is marking a Bandit false positive. The table 
name is validated and locked after initialization, and the values are 
parameterized to prevent SQL injection.
'''


class DefaultFormatsDB:
    """Database class for managing default format conversion mappings.

    Stores user-configured default output formats for given input formats.
    For example, a user can set png -> jpeg so that every time a PNG is
    uploaded, the output format dropdown defaults to JPEG.

    Attributes:
        settings: Application settings instance.
        DB_PATH: Path to the SQLite database file.
        _TABLE_NAME: Name of the database table for default formats.
        conn: Active SQLite database connection.
    """

    settings = get_settings()
    DB_PATH = settings.db_path
    _TABLE_NAME = "DEFAULT_FORMATS"

    @property
    def TABLE_NAME(self) -> str:
        """str: The validated, immutable table name."""
        return self._table_name

    def __init__(self) -> None:
        """Initialize DefaultFormatsDB, validate the table name, and create tables."""
        object.__setattr__(self, '_table_name', validate_sql_identifier(self._TABLE_NAME))
        self._local = threading.local()
        self.create_tables()

    @property
    def conn(self) -> sqlite3.Connection:
        """Return a thread-local SQLite connection, creating one if needed."""
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.DB_PATH)
        return self._local.conn

    def create_tables(self) -> None:
        """Create the default formats table if it does not already exist."""
        with self.conn:
            self.conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                    input_format  TEXT PRIMARY KEY,
                    output_format TEXT NOT NULL
                )
            """)  # nosec B608

    def get_all(self) -> list[dict]:
        """Return all default format mappings.

        Returns:
            A list of dicts with keys input_format and output_format.
        """
        cursor = self.conn.cursor()
        cursor.row_factory = sqlite3.Row
        cursor.execute(
            f"SELECT input_format, output_format FROM {self.TABLE_NAME} ORDER BY input_format",  # nosec B608
        )
        return [dict(row) for row in cursor.fetchall()]

    def get(self, input_format: str) -> dict | None:
        """Return the default output format for a given input format.

        Args:
            input_format: The input format to look up.

        Returns:
            A dict with input_format and output_format, or None.
        """
        cursor = self.conn.cursor()
        cursor.row_factory = sqlite3.Row
        cursor.execute(
            f"SELECT input_format, output_format FROM {self.TABLE_NAME} WHERE input_format = ?",  # nosec B608
            (input_format,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def upsert(self, input_format: str, output_format: str) -> dict:
        """Insert or update a default format mapping.

        Args:
            input_format: The input file format (e.g. "png").
            output_format: The default output format (e.g. "jpeg").

        Returns:
            A dict with input_format and output_format.
        """
        with self.conn:
            self.conn.execute(
                f"INSERT INTO {self.TABLE_NAME} (input_format, output_format) "  # nosec B608
                f"VALUES (?, ?) "
                f"ON CONFLICT(input_format) DO UPDATE SET output_format = excluded.output_format",
                (input_format, output_format)
            )
        return {"input_format": input_format, "output_format": output_format}

    def delete(self, input_format: str) -> bool:
        """Delete a default format mapping.

        Args:
            input_format: The input format to remove.

        Returns:
            True if a row was deleted, False otherwise.
        """
        with self.conn:
            cursor = self.conn.execute(
                f"DELETE FROM {self.TABLE_NAME} WHERE input_format = ?",  # nosec B608
                (input_format,)
            )
        return cursor.rowcount > 0

    def delete_all(self) -> int:
        """Delete all default format mappings.

        Returns:
            The number of rows deleted.
        """
        with self.conn:
            cursor = self.conn.execute(
                f"DELETE FROM {self.TABLE_NAME}",  # nosec B608
            )
        return cursor.rowcount

    def close(self) -> None:
        """Close the current thread's database connection."""
        if hasattr(self._local, 'conn') and self._local.conn:
            self._local.conn.close()
            self._local.conn = None
