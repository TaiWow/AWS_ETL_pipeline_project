import importlib
import json
import sys
import types
import unittest
from unittest.mock import Mock


def import_redshift_db_connection():
    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.client = Mock(side_effect=lambda *args, **kwargs: Mock())
    fake_psycopg2 = types.ModuleType("psycopg2")
    fake_psycopg2.connect = Mock()

    with unittest.mock.patch.dict(sys.modules, {"boto3": fake_boto3, "psycopg2": fake_psycopg2}):
        sys.modules.pop("AWS_ETL.redshift_db_connection", None)
        return importlib.import_module("AWS_ETL.redshift_db_connection")


redshift_db_connection = import_redshift_db_connection()


class RedshiftDbConnectionTests(unittest.TestCase):
    def test_get_ssm_param_parses_json_value(self):
        expected = {
            "host": "example.redshift.amazonaws.com",
            "user": "team_user",
            "database-name": "team_db",
            "password": "secret",
            "port": 5439,
        }
        redshift_db_connection.ssm_client.get_parameter = Mock(
            return_value={"Parameter": {"Value": json.dumps(expected)}}
        )

        actual = redshift_db_connection.get_ssm_param("team/redshift")

        self.assertEqual(expected, actual)
        redshift_db_connection.ssm_client.get_parameter.assert_called_once_with(Name="team/redshift")

    def test_open_sql_database_connection_and_cursor_uses_redshift_details(self):
        fake_cursor = Mock()
        fake_connection = Mock()
        fake_connection.cursor.return_value = fake_cursor
        redshift_db_connection.psy.connect = Mock(return_value=fake_connection)
        redshift_details = {
            "host": "example.redshift.amazonaws.com",
            "database-name": "team_db",
            "user": "team_user",
            "password": "secret",
            "port": 5439,
        }

        connection, cursor = redshift_db_connection.open_sql_database_connection_and_cursor(redshift_details)

        self.assertIs(fake_connection, connection)
        self.assertIs(fake_cursor, cursor)
        redshift_db_connection.psy.connect.assert_called_once_with(
            host="example.redshift.amazonaws.com",
            database="team_db",
            user="team_user",
            password="secret",
            port=5439,
        )


if __name__ == "__main__":
    unittest.main()
