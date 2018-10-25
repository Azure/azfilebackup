"""
Tests for the tarsnapshot module.
"""

from tests.loggedtestcase import LoggedTestCase
from azfilebak.tarsnapshot import Snapshot

class TestTarSnapshot(LoggedTestCase):
    """Test the tarsnapshot module."""

    def test_read_snapshot(self):
        """Test Snapshot."""
        snap = Snapshot('/tmp/test.snapshot')
        self.assertEqual(snap.header, 'GNU tar-1.30-2')
