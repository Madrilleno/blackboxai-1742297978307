import os
import json
import pytest
from unittest.mock import Mock, patch
from access_parser import AccessParser
from sharepoint_connector import SharePointConnector
from data_migration import DataMigration

# Test configuration
TEST_CONFIG = {
    "access_db": {
        "file_path": "test_db.accdb"
    },
    "sharepoint": {
        "site_url": "https://test.sharepoint.com",
        "client_id": "test-client-id",
        "client_secret": "test-client-secret",
        "tenant_id": "test-tenant-id"
    },
    "migration_settings": {
        "batch_size": 100,
        "retry_count": 2,
        "log_level": "INFO"
    }
}

@pytest.fixture
def config_file(tmp_path):
    """Create a temporary config file for testing."""
    config_path = tmp_path / "test_config.json"
    with open(config_path, "w") as f:
        json.dump(TEST_CONFIG, f)
    return str(config_path)

@pytest.fixture
def mock_access_parser():
    """Create a mock AccessParser instance."""
    with patch('access_parser.AccessParser') as mock:
        parser = mock.return_value
        parser.connect_to_access.return_value = True
        parser.extract_tables.return_value = [
            {
                "name": "TestTable",
                "columns": [
                    {"name": "ID", "type": "INTEGER", "nullable": False},
                    {"name": "Name", "type": "TEXT", "nullable": True},
                    {"name": "CreatedDate", "type": "DATETIME", "nullable": True}
                ],
                "primary_keys": ["ID"]
            }
        ]
        yield parser

@pytest.fixture
def mock_sharepoint_connector():
    """Create a mock SharePointConnector instance."""
    with patch('sharepoint_connector.SharePointConnector') as mock:
        connector = mock.return_value
        connector.authenticate_sharepoint.return_value = True
        connector.create_list.return_value = True
        connector.insert_items.return_value = True
        yield connector

class TestAccessParser:
    """Test cases for AccessParser class."""
    
    def test_initialization(self, config_file):
        """Test AccessParser initialization."""
        parser = AccessParser(config_file)
        assert parser is not None
        assert parser.config == TEST_CONFIG

    def test_connect_to_access(self, mock_access_parser, config_file):
        """Test connection to Access database."""
        parser = mock_access_parser
        assert parser.connect_to_access() is True

    def test_extract_tables(self, mock_access_parser, config_file):
        """Test table extraction from Access database."""
        parser = mock_access_parser
        tables = parser.extract_tables()
        assert len(tables) > 0
        assert "name" in tables[0]
        assert "columns" in tables[0]

class TestSharePointConnector:
    """Test cases for SharePointConnector class."""
    
    def test_initialization(self, config_file):
        """Test SharePointConnector initialization."""
        connector = SharePointConnector(config_file)
        assert connector is not None
        assert connector.config == TEST_CONFIG

    def test_authentication(self, mock_sharepoint_connector, config_file):
        """Test SharePoint authentication."""
        connector = mock_sharepoint_connector
        assert connector.authenticate_sharepoint() is True

    def test_create_list(self, mock_sharepoint_connector, config_file):
        """Test SharePoint list creation."""
        connector = mock_sharepoint_connector
        result = connector.create_list(
            "TestList",
            [{"name": "TestColumn", "type": "TEXT", "nullable": True}]
        )
        assert result is True

class TestDataMigration:
    """Test cases for DataMigration class."""
    
    def test_initialization(self, config_file):
        """Test DataMigration initialization."""
        migration = DataMigration(config_file)
        assert migration is not None
        assert migration.config == TEST_CONFIG

    @patch('data_migration.AccessParser')
    @patch('data_migration.SharePointConnector')
    def test_migrate_database(self, mock_sp, mock_access, config_file):
        """Test complete database migration process."""
        # Setup mocks
        mock_access.return_value.connect_to_access.return_value = True
        mock_access.return_value.extract_tables.return_value = [
            {
                "name": "TestTable",
                "columns": [
                    {"name": "ID", "type": "INTEGER", "nullable": False},
                    {"name": "Name", "type": "TEXT", "nullable": True}
                ],
                "primary_keys": ["ID"]
            }
        ]
        mock_sp.return_value.authenticate_sharepoint.return_value = True
        mock_sp.return_value.create_list.return_value = True
        mock_sp.return_value.insert_items.return_value = True

        # Run migration
        migration = DataMigration(config_file)
        result = migration.migrate_database()
        
        assert result is True
        mock_access.return_value.connect_to_access.assert_called_once()
        mock_sp.return_value.authenticate_sharepoint.assert_called_once()
        mock_sp.return_value.create_list.assert_called()

    def test_transform_data(self, config_file):
        """Test data transformation logic."""
        migration = DataMigration(config_file)
        
        # Test data
        test_data = [
            {
                "ID": 1,
                "Name": "Test Item",
                "IsActive": True,
                "CreatedDate": "2023-01-01"
            }
        ]
        
        # Column definitions
        columns = [
            {"name": "ID", "type": "INTEGER", "nullable": False},
            {"name": "Name", "type": "TEXT", "nullable": True},
            {"name": "IsActive", "type": "BOOLEAN", "nullable": True},
            {"name": "CreatedDate", "type": "DATETIME", "nullable": True}
        ]
        
        # Transform data
        result = migration._transform_data(test_data, columns)
        
        assert len(result) == 1
        assert result[0]["ID"] == 1
        assert result[0]["Name"] == "Test Item"
        assert result[0]["IsActive"] is True

def test_end_to_end_mock(config_file, mock_access_parser, mock_sharepoint_connector):
    """Test end-to-end migration process with mocked components."""
    # Initialize migration
    migration = DataMigration(config_file)
    
    # Mock the internal components
    migration.access_parser = mock_access_parser
    migration.sharepoint_connector = mock_sharepoint_connector
    
    # Run migration
    result = migration.migrate_database()
    
    # Verify the process
    assert result is True
    mock_access_parser.connect_to_access.assert_called_once()
    mock_access_parser.extract_tables.assert_called_once()
    mock_sharepoint_connector.authenticate_sharepoint.assert_called_once()
    mock_sharepoint_connector.create_list.assert_called()

if __name__ == '__main__':
    pytest.main([__file__])