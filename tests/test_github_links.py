import unittest

from serve import _has_github_link, _normalize_url


class GithubLinkDetectionTests(unittest.TestCase):
    def test_github_url_field_detected(self):
        paper = {"github_url": "https://github.com/example/repo"}
        self.assertTrue(_has_github_link(paper))

    def test_non_github_repository_is_ignored(self):
        paper = {"repository_url": "https://gitlab.com/example/repo"}
        self.assertFalse(_has_github_link(paper))

    def test_github_in_comment_text_detected(self):
        paper = {"comment": "Code available at github.com/example/repo"}
        self.assertTrue(_has_github_link(paper))

    def test_links_collection_with_github(self):
        paper = {"links": [{"href": "https://example.com"}, {"href": "http://github.com/example/repo"}]}
        self.assertTrue(_has_github_link(paper))

    def test_comment_with_trailing_punctuation(self):
        paper = {"comment": "See the repository: https://github.com/example/repo."}
        self.assertTrue(_has_github_link(paper))

    def test_github_word_without_url_not_detected(self):
        paper = {"summary": "This paper discusses GitHub usage."}
        self.assertFalse(_has_github_link(paper))

    def test_url_normalization_adds_scheme(self):
        normalized = _normalize_url("github.com/example/repo/")
        self.assertEqual(normalized, "https://github.com/example/repo/")


if __name__ == "__main__":
    unittest.main()
