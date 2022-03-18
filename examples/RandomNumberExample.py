#!/usr/bin/python

from copy import deepcopy
from functools import partial
from utils.QueryUtil import QueryUtil
from utils.WriteUtil import WriteUtil
from threading import Thread
from itertools import islice
import multiprocessing
from multiprocessing.pool import ThreadPool
import numpy as np
import time

NUM_PROCESSES = 32


class RandomNumberExample:
    def __init__(
        self,
        database_name,
        table_name,
        write_client,
        query_client,
        skip_deletion,
        multi_thread=True,
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
        self.multi_thread = multi_thread

    def generate_records(self):
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
        return records

    def bulk_write_records(self):
        records = self.generate_records()

        def group_elements(lst, chunk_size):
            lst = iter(lst)
            return iter(lambda: tuple(islice(lst, chunk_size)), ())

        batches = list(group_elements(records, len(records) // NUM_PROCESSES))
        with ThreadPool(NUM_PROCESSES) as pool:
            pool.map(partial(self.write_batch, batches), list(range(NUM_PROCESSES)))

    def write_batch(self, batches, batch_idx):
        records = []
        for i in range(len(batches[batch_idx])):
            records.append(batches[batch_idx][i])
            if len(records) == 100:
                self.__submit_batch(records, i)
                records = []
        print("Ingested %d records" % len(batches[batch_idx]))

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
            stime = time.time()
            self.bulk_write_records()
            etime = time.time()
            print(
                f"------------------ Bulk write takes {etime-stime} -------------------"
            )
            # self.run_sample_queries()

        finally:
            if not self.skip_deletion:
                self.write_util.delete_table(self.database_name, self.table_name)
                self.write_util.delete_database(self.database_name)
