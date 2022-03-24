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

MILLION = 10**6
MAX_BATCH_SIZE = 100
TOTAL_DURATION = 10 * (10**9)  # 500s
START_TIME = 1648040389000000000  # nanoseconds

COMMON_SLICE_RECORD_ATTRIBUTES = {
    "TimeUnit": "NANOSECONDS",
    "MeasureName": "kernel_name",
    "MeasureValueType": "VARCHAR",
}


class RandomNumberExample:
    def __init__(
        self,
        database_name,
        table_name,
        write_client,
        query_client,
        skip_deletion,
        num_thread=1,
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
        self.num_thread = num_thread

    def create_a_counter_record(self, i, current_time, time_offset):
        record = {
            "Dimensions": [
                {"Name": "id", "Value": str(i)},
            ],
            "Time": str(current_time - time_offset),
        }

        record.update(
            {
                "MeasureName": "utilization",
                "MeasureValueType": "DOUBLE",
                "MeasureValue": str(np.random.randint(100)),
            }
        )
        return record

    def create_a_slice_record(self, i, current_time, time_offset):
        record = {
            "Dimensions": [
                {"Name": "dur", "Value": str(i)},
                {"Name": "id", "Value": str(i)},
            ],
            "Time": str(current_time + time_offset),
            "MeasureValue": "k" + str(i),
        }

        return record

    def generate_records(self, num_records):
        records = []
        current_time = START_TIME
        # extracting each data row one by one
        idxes = np.arange(num_records)
        # np.random.shuffle(idxes)
        for i in range(num_records):
            idx = idxes[i]
            records.append(
                self.create_a_slice_record(
                    i, current_time, TOTAL_DURATION // num_records * idx
                )
            )
        return np.array(records)

    def bulk_write_records(self, num_records):
        loops = max(1, num_records // MILLION)
        num_records_per_loop = min(num_records, MILLION)
        for i in range(1, loops + 1):
            records = self.generate_records(num_records_per_loop)
            batches = np.array_split(records, self.num_thread)
            with ThreadPool(self.num_thread) as pool:
                pool.map(
                    partial(self.write_batch, batches), list(range(self.num_thread))
                )
            print("Ingested %d records" % num_records_per_loop)

    def write_batch(self, batches, batch_idx):
        cur_batch = batches[batch_idx]
        num_split = len(cur_batch) // MAX_BATCH_SIZE + (
            len(cur_batch) % MAX_BATCH_SIZE != 0
        )
        for i, batch in enumerate(np.array_split(cur_batch, num_split)):
            self.__submit_batch(batch.tolist(), (i + 1) * MAX_BATCH_SIZE)

    def __submit_batch(self, records, n):
        result = None
        try:
            result = self.write_client.write_records(
                DatabaseName=self.database_name,
                TableName=self.table_name,
                Records=records,
                CommonAttributes=COMMON_SLICE_RECORD_ATTRIBUTES,
            )
            # if result and result["ResponseMetadata"]:
            #     print(
            #         "Processed [%d] records. WriteRecords Status: [%s]"
            #         % (n, result["ResponseMetadata"]["HTTPStatusCode"])
            #     )
        except Exception as err:
            print("Error:", err)

    def run_sample_queries(self, num_records):
        query_string = f"SELECT COUNT(*) as NUM_OF_ROWS from {self.database_name}.{self.table_name}"
        # self.query_util.run_query(query_string, self.table_name)

        query_string = (
            f"SELECT max(time), min(time) from {self.database_name}.{self.table_name}"
        )
        # self.query_util.run_query(query_string, self.table_name)

        # Select 1500 records step time
        step_time = TOTAL_DURATION // num_records * max(1, num_records // 1000)  # ns
        query_string = " ".join(
            [
                f"WITH t as (SELECT MAX(dur) as dur, BIN(time, {step_time}ns) as bin_ts FROM {self.database_name}.{self.table_name}",
                f"WHERE time between from_nanoseconds({START_TIME}) - (interval '10' day) and from_nanoseconds({START_TIME}) + (interval '10' day)",
                f"GROUP BY BIN(time, {step_time}ns) ORDER BY bin_ts ASC)",
                f"SELECT m.id, m.measure_value::varchar, t.dur, t.bin_ts FROM {self.database_name}.{self.table_name} AS m INNER JOIN t",
                f"ON m.dur = t.dur",
            ]
        )

        print(query_string)
        self.query_util.run_query(query_string, self.table_name)

        # Try cancelling a query
        # self.query_util.cancel_query()

        # Try a query with multiple pages
        # self.query_util.run_query_with_multiple_pages(20000)

    def run(self, num_records):
        try:
            # Create base table and ingest records
            self.write_util.create_database(self.database_name)
            self.write_util.create_table(self.database_name, self.table_name)
            stime = time.time()
            self.bulk_write_records(num_records)
            etime = time.time()
            print(self.table_name, " Write time: ", etime - stime)
            self.run_sample_queries(num_records)

        finally:
            if not self.skip_deletion:
                self.write_util.delete_table(self.database_name, self.table_name)
                self.write_util.delete_database(self.database_name)
