import types
import sys
import unittest


def _stub_module(name, **attrs):
  module = types.ModuleType(name)
  for key, val in attrs.items():
    setattr(module, key, val)
  sys.modules[name] = module
  return module


class _DummyFlask:

  def __init__(self, *args, **kwargs):
    self.config = types.SimpleNamespace(from_object=lambda *a, **k: None)

  def route(self, *args, **kwargs):
    def decorator(func):
      return func
    return decorator

  def before_request(self, func):
    return func

  def teardown_request(self, func):
    return func


class _DummyLimiter:

  def __init__(self, *args, **kwargs):
    self.limit = lambda *a, **k: (lambda f: f)


_stub_module('numpy')
flask_stub = _stub_module(
  'flask',
  Flask=_DummyFlask,
  request=None,
  url_for=None,
  redirect=None,
  render_template=None,
  abort=None,
  g=None,
  flash=None,
  _app_ctx_stack=None,
  jsonify=None,
)
_stub_module('flask_limiter', Limiter=_DummyLimiter)
_stub_module('pymongo')
_stub_module('ingest_single_paper', ingest_paper=lambda *args, **kwargs: None)

from serve import _publication_statuses


class PublicationStatusExtractionTest(unittest.TestCase):

  def test_acceptance_and_presentation_from_comments(self):
    paper = {
      'comments': 'Accepted to NeurIPS 2024 as oral presentation. Camera-ready available soon.'
    }
    statuses = set(_publication_statuses(paper))
    self.assertIn('accepted', statuses)
    self.assertIn('presented', statuses)

  def test_venue_year_implies_acceptance(self):
    paper = {
      'comment': 'WACV 2025. Project page: https://example.com/project'
    }
    statuses = set(_publication_statuses(paper))
    self.assertIn('accepted', statuses)

  def test_presented_language_marks_presentation(self):
    paper = {
      'annotations': ['Presented at ICML 2023 workshop as poster.']
    }
    statuses = set(_publication_statuses(paper))
    self.assertIn('presented', statuses)
    self.assertIn('accepted', statuses)

  def test_journal_reference_to_appear(self):
    paper = {
      'journal_ref': 'To appear in IEEE TPAMI 2023.'
    }
    statuses = set(_publication_statuses(paper))
    self.assertIn('accepted', statuses)


if __name__ == '__main__':
  unittest.main()
