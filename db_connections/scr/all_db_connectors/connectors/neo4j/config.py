"""Neo4j-specific configuration."""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from db_connections.scr.all_db_connectors.core import BasePoolConfig


@dataclass
class Neo4jPoolConfig(BasePoolConfig):
    """Configuration for Neo4j connection pool.
    
    Extends BasePoolConfig with Neo4j-specific settings.
    """
    
    # Connection parameters
    host: str = "localhost"
    """Neo4j host address."""
    
    port: int = 7687
    """Neo4j port (7687 for bolt, 7474 for http)."""
    
    database: str = "neo4j"
    """Database name."""
    
    username: Optional[str] = None
    """Neo4j username."""
    
    password: Optional[str] = None
    """Neo4j password."""
    
    # Connection string alternative
    connection_url: Optional[str] = None
    """Complete Neo4j connection URL (overrides individual parameters if provided).
    
    Format: bolt://[username:password@]host[:port][/database]
    or: bolt+s://[username:password@]host[:port][/database] (for SSL)
    or: neo4j://[username:password@]host[:port][/database] (routing)
    """
    
    # Connection behavior
    timeout: int = 30
    """Query timeout in seconds."""
    
    connection_timeout: int = 10
    """Connection timeout in seconds."""
    
    max_retry_time: Optional[int] = None
    """Maximum retry time in seconds."""
    
    # SSL/TLS settings
    encrypted: bool = False
    """Enable SSL/TLS encryption."""
    
    trust: Optional[str] = None
    """Trust strategy for SSL certificates.
    
    Valid values:
    - TRUST_ALL_CERTIFICATES: Trust all certificates
    - TRUST_SYSTEM_CA_SIGNED_CERTIFICATES: Trust system CA signed certificates
    - TRUST_CUSTOM_CA_SIGNED_CERTIFICATES: Trust custom CA signed certificates
    """
    
    trusted_certificate: Optional[str] = None
    """Path to trusted certificate file."""
    
    # Scheme selection
    use_neo4j_scheme: bool = False
    """Use neo4j:// scheme for routing instead of bolt://."""
    
    use_bolt: Optional[bool] = None
    """Force use of bolt:// scheme."""
    
    use_http: Optional[bool] = None
    """Force use of http:// scheme."""
    
    # Pool settings (aliases for base class compatibility)
    max_connections: Optional[int] = field(default=None, repr=False, init=True)
    """Maximum number of connections (alias for max_size)."""
    
    min_connections: Optional[int] = field(default=None, repr=False, init=True)
    """Minimum number of connections (alias for min_size)."""
    
    max_idle_time: Optional[int] = field(default=None, repr=False, init=True)
    """Max idle time in seconds (alias for idle_timeout)."""
    
    # Neo4j-specific settings
    max_transaction_retry_time: Optional[int] = None
    """Maximum transaction retry time in seconds."""
    
    user_agent: Optional[str] = None
    """User agent string for connection tracking."""
    
    # Routing settings
    routing: Optional[bool] = None
    """Enable cluster routing."""
    
    routing_context: Optional[Dict[str, Any]] = None
    """Routing context for cluster routing."""
    
    # Connection pool settings
    connection_acquisition_timeout: Optional[int] = None
    """Connection acquisition timeout in seconds."""
    
    max_connection_lifetime: Optional[int] = None
    """Maximum connection lifetime in seconds."""
    
    # Keep-alive settings
    keep_alive: bool = True
    """Enable TCP keep-alive."""
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        # Sync alias fields with base class fields
        if self.max_connections is not None:
            self.max_size = self.max_connections
        else:
            self.max_connections = self.max_size
        
        if self.min_connections is not None:
            self.min_size = self.min_connections
        else:
            self.min_connections = self.min_size
        
        if self.max_idle_time is not None:
            self.idle_timeout = self.max_idle_time
        else:
            self.max_idle_time = self.idle_timeout
        
        # Validate connection parameters if not using connection URL
        if not self.connection_url:
            if not self.host:
                raise ValueError("host is required when not using connection_url")
            
            # Validate port
            if not (1 <= self.port <= 65535):
                raise ValueError("port must be between 1 and 65535")
        
        # Validate timeouts
        if self.timeout <= 0:
            raise ValueError("timeout must be positive")
        
        if self.connection_timeout <= 0:
            raise ValueError("connection_timeout must be positive")
        
        if self.max_retry_time is not None and self.max_retry_time <= 0:
            raise ValueError("max_retry_time must be positive if provided")
        
        # Note: Pool size validation (max_connections, min_connections, min <= max)
        # is deferred to validate_pool_config() which is called during pool creation.
        # This allows tests to create invalid configs and verify that pool.__init__
        # validates them with Neo4j-specific error messages.
        
        # Validate max_idle_time
        if self.idle_timeout <= 0:
            raise ValueError("max_idle_time must be positive")
        
        # Validate max_lifetime
        if self.max_lifetime <= 0:
            raise ValueError("max_lifetime must be positive")
        
        # Validate trust setting
        if self.trust is not None:
            valid_trusts = [
                "TRUST_ALL_CERTIFICATES",
                "TRUST_SYSTEM_CA_SIGNED_CERTIFICATES",
                "TRUST_CUSTOM_CA_SIGNED_CERTIFICATES"
            ]
            if self.trust not in valid_trusts:
                raise ValueError(
                    f"Invalid trust value. Must be one of: {', '.join(valid_trusts)}"
                )
    
    def get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters as a dictionary.
        
        Returns:
            Dictionary of connection parameters suitable for neo4j.GraphDatabase.driver().
        """
        params: Dict[str, Any] = {}
        
        if self.connection_url:
            params["uri"] = self.connection_url
        else:
            # Build URI based on scheme
            if self.use_http:
                scheme = "http"
            elif self.use_bolt:
                scheme = "bolt"
            elif self.use_neo4j_scheme:
                scheme = "neo4j"
            else:
                scheme = "bolt"
            
            uri = f"{scheme}://{self.host}:{self.port}"
            params["uri"] = uri
        
        # Authentication
        if self.username and self.password:
            params["auth"] = (self.username, self.password)
        elif self.username:
            params["auth"] = (self.username, "")
        elif self.password:
            # Include auth even if only password is set
            params["auth"] = ("", self.password)
        
        # Database
        params["database"] = self.database
        
        # Timeouts
        if self.connection_timeout:
            params["connection_timeout"] = self.connection_timeout
        
        if self.max_retry_time is not None:
            params["max_retry_time"] = self.max_retry_time
        
        # SSL/TLS
        if self.encrypted:
            params["encrypted"] = True
            if self.trust:
                params["trust"] = self.trust
            if self.trusted_certificate:
                params["trusted_certificate"] = self.trusted_certificate
        
        # Transaction retry
        if self.max_transaction_retry_time is not None:
            params["max_transaction_retry_time"] = self.max_transaction_retry_time
        
        # User agent
        if self.user_agent:
            params["user_agent"] = self.user_agent
        
        # Routing
        if self.routing is not None:
            params["routing"] = self.routing
        
        if self.routing_context is not None:
            params["routing_context"] = self.routing_context
        
        # Connection pool settings
        if self.connection_acquisition_timeout is not None:
            params["connection_acquisition_timeout"] = self.connection_acquisition_timeout
        
        if self.max_connection_lifetime is not None:
            params["max_connection_lifetime"] = self.max_connection_lifetime
        
        # Keep-alive
        if self.keep_alive is not None:
            params["keep_alive"] = self.keep_alive
        
        return params
    
    def get_connection_url(self) -> str:
        """Build a Neo4j connection URL.
        
        Returns:
            Connection string in URL format.
        """
        if self.connection_url:
            return self.connection_url
        
        # Determine scheme
        if self.use_http:
            scheme = "http"
        elif self.use_bolt:
            scheme = "bolt"
        elif self.use_neo4j_scheme:
            scheme = "neo4j"
        elif self.encrypted:
            scheme = "bolt+s"
        else:
            scheme = "bolt"
        
        # Build auth part
        auth = ""
        if self.username and self.password:
            auth = f"{self.username}:{self.password}@"
        elif self.username:
            auth = f"{self.username}@"
        
        # Build URL
        url = f"{scheme}://{auth}{self.host}:{self.port}/{self.database}"
        
        return url
    
    @classmethod
    def from_url(cls, url: str, **kwargs) -> "Neo4jPoolConfig":
        """Create configuration from a Neo4j connection URL.
        
        Args:
            url: Neo4j connection URL.
            **kwargs: Additional configuration parameters.
        
        Returns:
            Neo4jPoolConfig instance.
        """
        return cls(connection_url=url, **kwargs)
    
    @classmethod
    def from_env(cls, prefix: str = "NEO4J_") -> "Neo4jPoolConfig":
        """Create configuration from environment variables.
        
        Args:
            prefix: Environment variable prefix (default: NEO4J_).
        
        Returns:
            Neo4jPoolConfig instance.
            
        Example:
            Set environment variables:
            - NEO4J_HOST=localhost
            - NEO4J_PORT=7687
            - NEO4J_DATABASE=neo4j
            - NEO4J_USERNAME=neo4j
            - NEO4J_PASSWORD=secret
            - NEO4J_CONNECTION_URL=bolt://localhost:7687/neo4j
        """
        import os
        
        return cls(
            host=os.getenv(f"{prefix}HOST", "localhost"),
            port=int(os.getenv(f"{prefix}PORT", "7687")),
            database=os.getenv(f"{prefix}DATABASE", "neo4j"),
            username=os.getenv(f"{prefix}USERNAME"),
            password=os.getenv(f"{prefix}PASSWORD"),
            connection_url=os.getenv(f"{prefix}CONNECTION_URL"),
            encrypted=os.getenv(f"{prefix}ENCRYPTED", "false").lower() == "true",
            use_neo4j_scheme=os.getenv(f"{prefix}USE_NEO4J_SCHEME", "false").lower() == "true",
        )

