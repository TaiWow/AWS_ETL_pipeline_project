import importlib
import sys
import types
import unittest
from unittest.mock import Mock


def import_extract_transform_lambda():
    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.client = Mock(side_effect=lambda *args, **kwargs: Mock())

    with unittest.mock.patch.dict(sys.modules, {"boto3": fake_boto3}):
        sys.modules.pop("Extract_transform_lambda", None)
        return importlib.import_module("Extract_transform_lambda")


extract_transform_lambda = import_extract_transform_lambda()


class ExtractTransformLambdaTests(unittest.TestCase):
    def test_csv_to_list_builds_dicts_with_expected_keys(self):
        csv_payload = (
            "01/09/2021 08:15,Leeds,Ada,Coffee - 2.50,2.50,Card,1111\n"
            "02/09/2021 09:00,London,Bob,Tea - 1.75,1.75,Cash,2222\n"
        )

        rows = extract_transform_lambda.csv_to_list(csv_payload)

        self.assertEqual(2, len(rows))
        self.assertEqual("Ada", rows[0]["customer_name"])
        self.assertEqual("Tea - 1.75", rows[1]["items"])

    def test_transform_data_creates_row_per_item_with_quantity(self):
        # The current transform keeps one output row per purchased item while
        # annotating duplicate products with the total quantity for that product.
        transformed = extract_transform_lambda.transform_data([
            {
                "date_time": "05/09/2021 14:30",
                "location": "Leeds",
                "customer_name": "Ada",
                "items": "Coffee - 2.50,Coffee - 2.50,Cake - 3.25",
                "total_spent": "8.25",
                "payment_method": "Card",
                "card_number": "1111",
            }
        ])

        self.assertEqual(3, len(transformed))
        self.assertEqual("2021-09-05", transformed[0]["transaction_date"])
        self.assertEqual([2, 2, 1], [item["quantity"] for item in transformed])
        self.assertEqual(["Coffee", "Coffee", "Cake"], [item["product_name"] for item in transformed])

    def test_save_transformed_data_to_s3_as_csv_uses_expected_key(self):
        extract_transform_lambda.s3 = Mock()
        transformed_data = [{
            "transaction_date": "2021-09-05",
            "transaction_time": "14:30",
            "location": "Leeds",
            "product_name": "Coffee",
            "product_price": 2.5,
            "quantity": 1,
            "total_spent": 2.5,
            "payment_method": "Card",
        }]

        new_key = extract_transform_lambda.save_transformed_data_to_s3_as_csv(
            transformed_data,
            "incoming/leeds.csv",
        )

        self.assertEqual("transformed/leeds_transformed.csv", new_key)
        extract_transform_lambda.s3.put_object.assert_called_once()
        _, kwargs = extract_transform_lambda.s3.put_object.call_args
        self.assertEqual(extract_transform_lambda.output_bucket_name, kwargs["Bucket"])
        self.assertIn("transaction_date", kwargs["Body"])

    def test_send_sqs_message_serializes_bucket_and_key(self):
        extract_transform_lambda.sqs = Mock()

        extract_transform_lambda.send_sqs_message("transformed/leeds_transformed.csv")

        extract_transform_lambda.sqs.send_message.assert_called_once()
        _, kwargs = extract_transform_lambda.sqs.send_message.call_args
        self.assertEqual(extract_transform_lambda.queue_url, kwargs["QueueUrl"])
        self.assertIn("transformed/leeds_transformed.csv", kwargs["MessageBody"])


if __name__ == "__main__":
    unittest.main()
