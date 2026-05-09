import os
import tempfile
import unittest

from etl_shared.transform import (
    csv_text_to_list,
    remove_sensitive_data,
    split_date_and_time,
    split_items_and_count_quantity,
    transform_data,
)
from Local_ETL.csv_transform import csv_to_list


SAMPLE_CSV_LINE = (
    '23/07/2024 09:00:00,Leeds,Jane Doe,'
    '"Latte - 3.5,Latte - 3.5,Espresso - 2.0",9.0,card,1234'
)


class TestTransformLibrary(unittest.TestCase):
    def test_csv_text_to_list_uses_expected_columns(self):
        rows = csv_text_to_list(SAMPLE_CSV_LINE)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["location"], "Leeds")
        self.assertEqual(rows[0]["customer_name"], "Jane Doe")

    def test_remove_sensitive_data_drops_customer_fields(self):
        rows = csv_text_to_list(SAMPLE_CSV_LINE)
        transformed = remove_sensitive_data(rows)
        self.assertIn("date_time", transformed[0])
        self.assertNotIn("customer_name", transformed[0])
        self.assertNotIn("card_number", transformed[0])

    def test_split_date_and_time_formats_date(self):
        rows = remove_sensitive_data(csv_text_to_list(SAMPLE_CSV_LINE))
        transformed = split_date_and_time(rows)
        self.assertEqual(transformed[0]["transaction_date"], "2024-07-23")
        self.assertEqual(transformed[0]["transaction_time"], "09:00:00")

    def test_split_items_and_count_quantity_counts_duplicate_items(self):
        rows = split_date_and_time(remove_sensitive_data(csv_text_to_list(SAMPLE_CSV_LINE)))
        transformed = split_items_and_count_quantity(rows)
        self.assertEqual(len(transformed), 3)

        latte_rows = [row for row in transformed if row["product_name"] == "Latte"]
        espresso_rows = [row for row in transformed if row["product_name"] == "Espresso"]

        self.assertEqual(len(latte_rows), 2)
        self.assertTrue(all(row["quantity"] == 2 for row in latte_rows))
        self.assertEqual(len(espresso_rows), 1)
        self.assertEqual(espresso_rows[0]["quantity"], 1)

    def test_transform_data_runs_full_pipeline(self):
        transformed = transform_data(csv_text_to_list(SAMPLE_CSV_LINE))
        self.assertEqual(len(transformed), 3)
        self.assertEqual(set(row["product_name"] for row in transformed), {"Latte", "Espresso"})

    def test_local_csv_transform_wrapper_reads_file_and_reuses_shared_logic(self):
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_csv:
            temp_csv.write(SAMPLE_CSV_LINE)
            temp_path = temp_csv.name

        try:
            rows = csv_to_list(temp_path)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["location"], "Leeds")
        finally:
            os.remove(temp_path)


if __name__ == "__main__":
    unittest.main()
