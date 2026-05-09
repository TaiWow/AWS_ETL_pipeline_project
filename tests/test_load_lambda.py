import importlib
import sys
import types
import unittest
from unittest.mock import Mock


class FakeCursor:
    def __init__(self, fetchone_results=None):
        self.fetchone_results = list(fetchone_results or [])
        self.execute_calls = []
        self.executemany_calls = []

    def execute(self, sql, params=None):
        self.execute_calls.append((sql, params))

    def executemany(self, sql, params_list):
        self.executemany_calls.append((sql, list(params_list)))

    def fetchone(self):
        if self.fetchone_results:
            return self.fetchone_results.pop(0)
        return None


def import_load_lambda():
    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.client = Mock(side_effect=lambda *args, **kwargs: Mock())
    fake_psycopg2 = types.ModuleType("psycopg2")
    fake_psycopg2.connect = Mock()

    with unittest.mock.patch.dict(sys.modules, {"boto3": fake_boto3, "psycopg2": fake_psycopg2}):
        sys.modules.pop("Load_lambda", None)
        return importlib.import_module("Load_lambda")


load_lambda = import_load_lambda()


class LoadLambdaTests(unittest.TestCase):
    def test_csv_to_list_uses_header_row(self):
        csv_payload = (
            "transaction_date,transaction_time,location,product_name,product_price,quantity,total_spent,payment_method\n"
            "2021-09-05,14:30,Leeds,Coffee,2.5,1,2.5,Card\n"
        )

        rows = load_lambda.csv_to_list(csv_payload)

        self.assertEqual(
            [{
                "transaction_date": "2021-09-05",
                "transaction_time": "14:30",
                "location": "Leeds",
                "product_name": "Coffee",
                "product_price": "2.5",
                "quantity": "1",
                "total_spent": "2.5",
                "payment_method": "Card",
            }],
            rows,
        )

    def test_insert_locations_batches_unique_new_locations(self):
        cursor = FakeCursor([None, None, (1,)])
        transformed_data = [
            {"location": "Leeds"},
            {"location": "York"},
            {"location": "Leeds"},
        ]

        load_lambda.insert_locations(cursor, transformed_data)

        self.assertEqual(1, len(cursor.executemany_calls))
        _, params = cursor.executemany_calls[0]
        self.assertEqual([("Leeds",), ("York",)], params)

    def test_insert_products_batches_unique_new_products(self):
        cursor = FakeCursor([None, None, (1,)])
        transformed_data = [
            {"product_name": "Coffee", "product_price": 2.5},
            {"product_name": "Cake", "product_price": 3.25},
            {"product_name": "Coffee", "product_price": 2.5},
        ]

        load_lambda.insert_products(cursor, transformed_data)

        self.assertEqual(1, len(cursor.executemany_calls))
        _, params = cursor.executemany_calls[0]
        self.assertEqual([("Coffee", 2.5), ("Cake", 3.25)], params)

    def test_insert_transactions_batches_unique_transactions(self):
        cursor = FakeCursor([
            (7,), None,
            (7,), None,
        ])
        transformed_data = [
            {
                "location": "Leeds",
                "transaction_date": "2021-09-05",
                "transaction_time": "14:30",
                "payment_method": "Card",
                "total_spent": 2.5,
            },
            {
                "location": "Leeds",
                "transaction_date": "2021-09-05",
                "transaction_time": "14:30",
                "payment_method": "Card",
                "total_spent": 2.5,
            },
        ]

        load_lambda.insert_transactions(cursor, transformed_data)

        self.assertEqual(1, len(cursor.executemany_calls))
        _, params = cursor.executemany_calls[0]
        self.assertEqual([("2021-09-05", "14:30", 7, "Card", 2.5)], params)

    def test_insert_orders_batches_unique_orders(self):
        cursor = FakeCursor([
            (10,), (7,), (20,), None,
            (10,), (7,), (20,), None,
        ])
        transformed_data = [
            {
                "product_name": "Coffee",
                "product_price": 2.5,
                "transaction_date": "2021-09-05",
                "transaction_time": "14:30",
                "quantity": 2,
                "location": "Leeds",
            },
            {
                "product_name": "Coffee",
                "product_price": 2.5,
                "transaction_date": "2021-09-05",
                "transaction_time": "14:30",
                "quantity": 2,
                "location": "Leeds",
            },
        ]

        load_lambda.insert_orders(cursor, transformed_data)

        self.assertEqual(1, len(cursor.executemany_calls))
        _, params = cursor.executemany_calls[0]
        self.assertEqual([(20, 10, 2)], params)


if __name__ == "__main__":
    unittest.main()
