import tempfile
import unittest

from Local_ETL import csv_transform


class CsvTransformTests(unittest.TestCase):
    def test_csv_to_list_reads_rows_using_expected_columns(self):
        csv_content = (
            "01/09/2021 08:15,Leeds,Ada,Coffee - 2.50,2.50,Card,1111\n"
            "02/09/2021 09:00,London,Bob,Tea - 1.75,1.75,Cash,2222\n"
        )

        with tempfile.NamedTemporaryFile("w+", delete=False) as temp_file:
            temp_file.write(csv_content)
            temp_file.flush()
            rows = csv_transform.csv_to_list(temp_file.name)

        self.assertEqual(2, len(rows))
        self.assertEqual("Leeds", rows[0]["location"])
        self.assertEqual("Cash", rows[1]["payment_method"])

    def test_remove_sensitive_data_excludes_customer_fields(self):
        transformed = csv_transform.remove_sensitive_data([
            {
                "date_time": "01/09/2021 08:15",
                "location": "Leeds",
                "customer_name": "Ada",
                "items": "Coffee - 2.50",
                "total_spent": "2.50",
                "payment_method": "Card",
                "card_number": "1111",
            }
        ])

        self.assertEqual(
            [{
                "date_time": "01/09/2021 08:15",
                "location": "Leeds",
                "items": "Coffee - 2.50",
                "total_spent": "2.50",
                "payment_method": "Card",
            }],
            transformed,
        )

    def test_split_date_and_time_normalizes_dates(self):
        transformed = csv_transform.split_date_and_time([
            {
                "date_time": "05/09/2021 14:30",
                "location": "Leeds",
                "items": "Coffee - 2.50",
                "total_spent": "2.50",
                "payment_method": "Card",
            }
        ])

        self.assertEqual("2021-09-05", transformed[0]["transaction_date"])
        self.assertEqual("14:30", transformed[0]["transaction_time"])
        self.assertEqual("05/09/2021 14:30", transformed[0]["date_time"])

    def test_split_items_and_count_quantity_expands_each_item(self):
        transformed = csv_transform.split_items_and_count_quantity([
            {
                "transaction_date": "2021-09-05",
                "transaction_time": "14:30",
                "location": "Leeds",
                "items": "Coffee - 2.50,Coffee - 2.50,Cake - 3.25",
                "total_spent": "8.25",
                "payment_method": "Card",
            }
        ])

        self.assertEqual(3, len(transformed))
        self.assertEqual(
            ["Coffee", "Coffee", "Cake"],
            [item["product_name"] for item in transformed],
        )
        self.assertEqual([2, 2, 1], [item["quantity"] for item in transformed])
        self.assertEqual([2.5, 2.5, 3.25], [item["product_price"] for item in transformed])
        self.assertTrue(all(item["total_spent"] == 8.25 for item in transformed))


if __name__ == "__main__":
    unittest.main()
