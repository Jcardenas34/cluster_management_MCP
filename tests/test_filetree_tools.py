
'''
Testing filetree exploration tool call for SLM.
Dedicated test module for modularity and isolation.
Usage:
    pytest -m filetree
'''
import pytest
from cluster_management_MCP.utils.cli_tools import ls_filetree


@pytest.mark.filetree
class TestLsFiletree:

    def test_lists_home_directory(self, mock_filetree):
        """Should return a non-empty list for a valid directory."""
        result = ls_filetree(str(mock_filetree))
        assert isinstance(result, list), "Returned object is not a list"
        assert len(result) > 0 , "Returned no content, empty folder?"

    def test_each_entry_has_required_keys(self, mock_filetree):
        """Every entry must have name, type, and path keys."""
        result = ls_filetree(str(mock_filetree))
        for entry in result:
            assert "name" in entry, "object name is not in entry"
            assert "type" in entry, "object type is not in entry"
            assert "path" in entry, "object path is not in entry"

    def test_type_values_are_valid(self, mock_filetree):
        """type field should be 'file' or 'directory'"""
        result = ls_filetree(str(mock_filetree))
        valid_types = {"file", "directory"}   
        for entry in result:
            assert entry["type"] in valid_types

    def test_recursive_returns_more_entries(self, mock_filetree):
        """Recursive listing should return at least as many entries as non-recursive."""
        non_recursive = ls_filetree(str(mock_filetree), recursive=False)
        recursive = ls_filetree(str(mock_filetree), recursive=True)
        assert len(recursive) >= len(non_recursive)

    def test_recursion_triggers(self, mock_filetree):
        '''file named risk_battle_results.txt must exist in results when recursive is on, and not exist when off.'''
        result = ls_filetree(mock_filetree, recursive=True)
        non_recursive_result = ls_filetree(mock_filetree)
        assert "risk_battle_results.txt" in [entry["name"] for entry in result]
        assert "risk_battle_results.txt" not in [entry["name"] for entry in non_recursive_result]

    def test_raises_on_missing_directory(self):
        """Should raise FileNotFoundError for a path that does not exist."""
        with pytest.raises(FileNotFoundError):
            ls_filetree("/this/path/does/not/exist/at/all")

    def test_raises_on_file_passed(self, mock_filetree):
        '''Should raise the NotADirectoryError when file is passed to the function'''
        file = mock_filetree/"risk_results_with_cease_fires.txt"
        with pytest.raises(NotADirectoryError):
            ls_filetree(file)
