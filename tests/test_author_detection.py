import unittest
from app.parser import parse_filename, build_merge_suggestions


class AuthorDetectionTests(unittest.TestCase):
    def test_prefix_suffix_bracket_cases(self):
        p1 = parse_filename("Asimov, Isaac - Foundation.epub")
        self.assertEqual(p1.author, "Isaac Asimov")
        self.assertGreaterEqual(p1.author_confidence, 0.70)

        p2 = parse_filename("[Isaac Asimov] Foundation (1951).pdf")
        self.assertEqual(p2.author, "Isaac Asimov")
        self.assertGreaterEqual(p2.author_confidence, 0.85)

        p3 = parse_filename("Foundation - Isaac Asimov.epub")
        self.assertEqual(p3.author, "Isaac Asimov")
        self.assertGreaterEqual(p3.author_confidence, 0.70)

    def test_multi_author_and_suffix(self):
        p = parse_filename("BLANCHET AÎNÉ, JÉRÔME & ROSNY JR, JOHN - Titre.epub")
        self.assertEqual(p.author, "Jérôme Blanchet Aîné & John Rosny Jr")
        self.assertIn("jerome blanchet aine", p.author_norm)

    def test_known_author_boost_deterministic(self):
        known = {
            "j k rowling": {"canonical": "J. K. Rowling", "frequency": 120},
            "harry potter 01": {"canonical": "Harry Potter 01", "frequency": 1},
        }
        p = parse_filename("Harry Potter 01 - J. K. Rowling.epub", known_authors=known)
        self.assertEqual(p.author, "J. K. Rowling")
        self.assertGreaterEqual(p.author_confidence, 0.70)

    def test_stopword_penalty(self):
        p = parse_filename("Complete Collection - Volume 2 - Some Author.epub")
        self.assertEqual(p.author, "Some Author")

    def test_merge_suggestions_progress_callback(self):
        known = [
            ("isaac asimov", "Isaac Asimov", 100),
            ("asimov isaac", "Asimov, Isaac", 10),
            ("i asimov", "I. Asimov", 5),
        ]
        events = []

        def cb(done, total):
            events.append((done, total))

        build_merge_suggestions(known, threshold=0.50, progress_cb=cb, progress_every=1)
        self.assertTrue(events)
        self.assertEqual(events[0][0], 0)
        self.assertEqual(events[-1][0], events[-1][1])
        self.assertEqual(events[-1][1], 3)

    def test_merge_suggestions_blocking_handles_no_candidates(self):
        known = [
            ("isaac asimov", "Isaac Asimov", 100),
            ("victor hugo", "Victor Hugo", 90),
        ]
        events = []

        def cb(done, total):
            events.append((done, total))

        suggestions = build_merge_suggestions(known, threshold=0.92, progress_cb=cb, progress_every=1)
        self.assertEqual(suggestions, [])
        self.assertTrue(events)
        self.assertEqual(events[0][0], 0)
        self.assertGreaterEqual(events[0][1], 0)

    def test_merge_suggestions(self):
        known = [
            ("isaac asimov", "Isaac Asimov", 100),
            ("asimov isaac", "Asimov, Isaac", 10),
            ("j k rowling", "J. K. Rowling", 80),
        ]
        suggestions = build_merge_suggestions(known, threshold=0.92)
        pairs = {(s.left_name, s.right_name) for s in suggestions} | {(s.right_name, s.left_name) for s in suggestions}
        self.assertIn(("Isaac Asimov", "Asimov, Isaac"), pairs)


if __name__ == "__main__":
    unittest.main()
