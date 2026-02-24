import os
import tempfile
import unittest

from app.db import DB, MEMORY_PROFILES


class CustomMemoryProfileTests(unittest.TestCase):
    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix='.sqlite')
        os.close(fd)
        self.db = DB(self.path)

    def tearDown(self):
        try:
            self.db.close()
        except Exception:
            pass
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_custom_profile_persisted(self):
        cfg = {
            'synchronous': 'FULL',
            'cache_size': -12345,
            'mmap_size': 123456,
            'wal_autocheckpoint': 321,
        }
        self.db.apply_memory_profile('custom', cfg)
        self.assertEqual(self.db.memory_profile(), 'custom')

        db2 = DB(self.path)
        try:
            self.assertEqual(db2.memory_profile(), 'custom')
            cfg2 = db2.memory_profile_config('custom')
            self.assertEqual(cfg2['synchronous'], 'FULL')
            self.assertEqual(int(cfg2['cache_size']), -12345)
            self.assertEqual(int(cfg2['mmap_size']), 123456)
            self.assertEqual(int(cfg2['wal_autocheckpoint']), 321)
        finally:
            db2.close()

    def test_invalid_custom_values_sanitized(self):
        self.db.apply_memory_profile('custom', {'synchronous': 'NOPE'})
        cfg = self.db.memory_profile_config('custom')
        self.assertEqual(cfg['synchronous'], 'NORMAL')
        self.assertEqual(int(cfg['cache_size']), int(MEMORY_PROFILES['balanced']['cache_size']))


if __name__ == '__main__':
    unittest.main()
