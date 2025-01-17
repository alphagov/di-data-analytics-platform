from __future__ import annotations

import logging
import time

from ..logger import logger
from ..strategies.Strategy import Strategy

METADATA_ROOT_FOLDER = "txma_raw_stage_metadata"


class RawToStageProcessor:

    def __init__(self, args: dict, strategy: Strategy) -> None:
        if strategy:
            self.strategy = strategy
        self.logger = logging.getLogger(__name__)
        logger.init(args)
        logger.configure(self.logger)

    def process(self) -> None:

        # extract data from raw layer
        dfs = self.strategy.extract()

        df_process_counter = 0
        cumulative_stage_table_rows_inserted = 0
        cumulative_stage_key_rows_inserted = 0
        cumulative_duplicate_rows_removed = 0

        # for each dataframe, transform and then load
        for df_raw in dfs:
            df_process_counter += 1
            self.logger.info("processing dataframe chunk: %s", df_process_counter)
            # Record the start time
            start_time = time.time()

            # Transform df chunk
            (df_stage, df_key_values, duplicate_rows_removed, stage_table_rows_inserted, stage_key_rows_inserted) = self.strategy.transform(df_raw)

            cumulative_duplicate_rows_removed = cumulative_duplicate_rows_removed + duplicate_rows_removed
            cumulative_stage_table_rows_inserted = cumulative_stage_table_rows_inserted + stage_table_rows_inserted
            cumulative_stage_key_rows_inserted = cumulative_stage_key_rows_inserted + stage_key_rows_inserted

            # Load transformed dfs
            self.strategy.load(df_stage, df_key_values)

            # Record the end time
            end_time = time.time()

            # Calculate the elapsed time in seconds
            elapsed_time = end_time - start_time

            # Convert the elapsed time to minutes
            elapsed_minutes = elapsed_time / 60

            # Print the result
            self.logger.info("Time taken to process dataframe %s: %2f minutes", df_process_counter, elapsed_minutes)
            self.logger.info("stage layer successfully updated")
            self.logger.info("total stage table records inserted: %s", cumulative_stage_table_rows_inserted)
            self.logger.info("total stage key table records inserted: %s", cumulative_stage_key_rows_inserted)
            self.logger.info("total duplicate rows removed: %s", cumulative_duplicate_rows_removed)
