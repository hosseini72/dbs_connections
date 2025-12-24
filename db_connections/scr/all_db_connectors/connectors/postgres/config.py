"""PostgreSQL-specific configuration."""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from db_connections.scr.all_db_connectors.core import BasePoolConfig 


@dataclass
class PostgresPoolConfig(BasePoolConfig):
    """Configuration for PostgreSQL connection pool.
    
    Extends BasePoolConfig with PostgreSQL-specific settings.
    """
    
    # Connection parameters
    host: str = "localhost"
    """Database host address."""
    
    port: int = 5432
    """Database port."""
    
    database: str = "postgres"
    """Database name."""
    
    user: str = "postgres"
    """Database user."""
    
    password: Optional[str] = None
    """Database password."""
    
    # SSL/TLS settings
    sslmode: str = "prefer"
    """SSL mode: disable, allow, prefer, require, verify-ca, verify-full."""
    
    sslcert: Optional[str] = None
    """Path to client certificate file."""
    
    sslkey: Optional[str] = None
    """Path to client private key file."""
    
    sslrootcert: Optional[str] = None
    """Path to root certificate file."""
    
    # Connection behavior
    connect_timeout: int = 10
    """Connection timeout in seconds."""
    
    command_timeout: Optional[int] = None
    """Command timeout in seconds (None = no timeout)."""
    
    server_settings: Optional[Dict[str, str]] = None
    """Server settings to apply on connection (e.g., timezone, search_path)."""
    
    # Application metadata
    application_name: Optional[str] = None
    """Application name for connection tracking."""
    
    # Advanced settings
    statement_cache_size: int = 100
    """Number of prepared statements to cache."""
    
    prepared_statement_cache_enabled: bool = True
    """Whether to enable prepared statement caching."""
    
    max_cached_statement_lifetime: int = 3600
    """Maximum lifetime of cached prepared statements in seconds."""
    
    # Connection string alternative
    connection_string: Optional[str] = None
    """Complete connection string (overrides individual parameters if provided)."""
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.connection_string:
            # Validate required fields if not using connection string
            if not self.host:
                raise ValueError("host is required when not using connection_string")
            if not self.database:
                raise ValueError("database is required when not using connection_string")
            if not self.user:
                raise ValueError("user is required when not using connection_string")
        
        # Validate SSL mode
        valid_ssl_modes = {
            "disable", "allow", "prefer", "require", 
            "verify-ca", "verify-full"
        }
        if self.sslmode not in valid_ssl_modes:
            raise ValueError(
                f"Invalid sslmode: {self.sslmode}. "
                f"Must be one of {valid_ssl_modes}"
            )
        
        # Initialize server_settings if None
        if self.server_settings is None:
            self.server_settings = {}
    
    def get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters as a dictionary.
        
        Returns:
            Dictionary of connection parameters suitable for psycopg2/asyncpg.
        """
        if self.connection_string:
            return {"dsn": self.connection_string}
        
        params = {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
        }
        
        if self.password:
            params["password"] = self.password
        
        # SSL parameters
        if self.sslmode != "prefer":  # Only add if not default
            params["sslmode"] = self.sslmode
        if self.sslcert:
            params["sslcert"] = self.sslcert
        if self.sslkey:
            params["sslkey"] = self.sslkey
        if self.sslrootcert:
            params["sslrootcert"] = self.sslrootcert
        
        # Timeouts
        params["connect_timeout"] = self.connect_timeout
        if self.command_timeout:
            params["command_timeout"] = self.command_timeout
        
        # Application name
        if self.application_name:
            params["application_name"] = self.application_name
        
        # Server settings
        if self.server_settings:
            params["server_settings"] = self.server_settings
        
        return params
    
    def get_dsn(self) -> str:
        """Build a PostgreSQL DSN connection string.
        
        Returns:
            Connection string in DSN format.
        """
        if self.connection_string:
            return self.connection_string
        
        # Build basic DSN
        password_part = f":{self.password}" if self.password else ""
        dsn = (
            f"postgresql://{self.user}{password_part}@"
            f"{self.host}:{self.port}/{self.database}"
        )
        
        # Add query parameters
        params = []
        if self.sslmode != "prefer":
            params.append(f"sslmode={self.sslmode}")
        if self.connect_timeout != 10:
            params.append(f"connect_timeout={self.connect_timeout}")
        if self.application_name:
            params.append(f"application_name={self.application_name}")
        
        if params:
            dsn += "?" + "&".join(params)
        
        return dsn
    
    @classmethod
    def from_dsn(cls, dsn: str, **kwargs) -> "PostgresPoolConfig":
        """Create configuration from a DSN connection string.
        
        Args:
            dsn: PostgreSQL connection string.
            **kwargs: Additional configuration parameters.
        
        Returns:
            PostgresPoolConfig instance.
        """
        return cls(connection_string=dsn, **kwargs)
    
    @classmethod
    def from_env(cls, prefix: str = "POSTGRES_") -> "PostgresPoolConfig":
        """Create configuration from environment variables.
        
        Args:
            prefix: Environment variable prefix (default: POSTGRES_).
        
        Returns:
            PostgresPoolConfig instance.
            
        Example:
            Set environment variables:
            - POSTGRES_HOST=localhost
            - POSTGRES_PORT=5432
            - POSTGRES_DATABASE=mydb
            - POSTGRES_USER=myuser
            - POSTGRES_PASSWORD=secret
        """
        import os
        
        return cls(
            host=os.getenv(f"{prefix}HOST", "localhost"),
            port=int(os.getenv(f"{prefix}PORT", "5432")),
            database=os.getenv(f"{prefix}DATABASE", "postgres"),
            user=os.getenv(f"{prefix}USER", "postgres"),
            password=os.getenv(f"{prefix}PASSWORD"),
            sslmode=os.getenv(f"{prefix}SSLMODE", "prefer"),
            application_name=os.getenv(f"{prefix}APPLICATION_NAME"),
            connection_string=os.getenv(f"{prefix}CONNECTION_STRING"),
        )