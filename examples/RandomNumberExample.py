#!/usr/bin/python

import csv
from utils.QueryUtil import QueryUtil
from utils.WriteUtil import WriteUtil
import numpy as np
import time


class RandomNumberExample:
    def __init__(
        self, database_name, table_name, write_client, query_client, skip_deletion
    ):
        self.database_name = database_name
        self.table_name = table_name
        self.write_client = write_client
        self.write_util = WriteUtil(self.write_client)
        self.query_client = query_client
        self.query_util = QueryUtil(
            self.query_client, self.database_name, self.table_name
        )
        self.skip_deletion = skip_deletion

    def bulk_write_records(self):
        records = []
        current_time = time.time() * 1000

        # extracting each data row one by one
        num_records = 1000000
        idxes = np.arange(num_records)
        np.random.shuffle(idxes)
        for i in range(num_records):
            idx = idxes[i]
            record = {
                "Dimensions": [
                    {"Name": "id", "Value": str(i)},
                    {"Name": "track_id", "Value": str(np.random.randint(10))},
                ],
                "Time": str(int(current_time - idx / 1000)),
            }

            record.update(
                {
                    "MeasureName": "utilization",
                    "MeasureValueType": "DOUBLE",
                    "MeasureValue": str(np.random.randint(100)),
                }
            )
            records.append(record)

            if len(records) == 100:
                self.__submit_batch(records, i)
                records = []

        print("Ingested %d records" % num_records)

    def __submit_batch(self, records, n):
        result = None
        try:
            result = self.write_client.write_records(
                DatabaseName=self.database_name,
                TableName=self.table_name,
                Records=records,
                CommonAttributes={},
            )
            if result and result["ResponseMetadata"]:
                print(
                    "Processed [%d] records. WriteRecords Status: [%s]"
                    % (n, result["ResponseMetadata"]["HTTPStatusCode"])
                )
        except Exception as err:
            print("Error:", err)

    def run_sample_queries(self):
        self.query_util.run_all_queries()

        # Try cancelling a query
        self.query_util.cancel_query()

        # Try a query with multiple pages
        self.query_util.run_query_with_multiple_pages(20000)

    def run(self):
        try:
            # Create base table and ingest records
            self.write_util.create_database(self.database_name)
            self.write_util.create_table(self.database_name, self.table_name)
            self.bulk_write_records()
            # self.run_sample_queries()

        finally:
            if not self.skip_deletion:
                self.write_util.delete_table(self.database_name, self.table_name)
                self.write_util.delete_database(self.database_name)
