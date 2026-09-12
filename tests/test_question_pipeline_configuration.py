"""Activation uses explicit booleans and remains disabled without configuration."""
import os
import unittest
from unittest.mock import patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app.config import Settings
from app.query import QueryEngine


class PipelineConfigurationTests(unittest.TestCase):
    def configured(self, value):
        with patch.dict(os.environ):
            os.environ.pop('QUESTION_PIPELINE_ENABLED', None)
            if value is not None:
                os.environ['QUESTION_PIPELINE_ENABLED'] = value
            return Settings(_env_file=None)

    def test_environment_activation_and_explicit_override_precedence(self):
        for setting, override, expected in (
            (None, None, False), ('false', None, False), ('true', None, True),
            ('true', False, False), ('false', True, True),
        ):
            with self.subTest(setting=setting, override=override), patch(
                    'app.query.settings', self.configured(setting)), patch('app.query.AsyncOpenAI'):
                engine = QueryEngine(question_pipeline=override)
                self.assertIs(engine.question_pipeline, expected)

    def test_invalid_override_is_rejected_before_client_creation(self):
        for value in ('false', 'true', 0, 1, [], {}):
            with self.subTest(value=value), patch('app.query.AsyncOpenAI') as client:
                with self.assertRaises(ValueError):
                    QueryEngine(question_pipeline=value)
                client.assert_not_called()
