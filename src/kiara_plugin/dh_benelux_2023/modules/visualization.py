# -*- coding: utf-8 -*-

from kiara.api import KiaraModule

import re
import pandas as pd
import duckdb

class VizDataQuery(KiaraModule):
    """
    This module processes a dataset to display a visualization of the corpus aggregated by a period of time.
    It aims at serving as a visual aid to create a subset of a table.
    """

    # _config_cls = ExampleModuleConfig
    _module_type_name = "viz_data_query"

    def create_inputs_schema(self):
        
        return {
             "distribution": {
                "type": "string",
                "doc": "The wished data periodicity to display on visualization, values can be either 'day','month' or 'year'."
            },
            "column": {
                "type": "string",
                "doc": "The column that contains publication names or ref/id."
            },
            "table": {
                "type": "table",
                "doc": "The table for which the distribution is needed."
            }
        }

    def create_outputs_schema(self):
        return {
            "viz_data": {
                "type": "list",
                "doc": "The aggregated data as a list of dicts for visualization purposes."
            }
        }

    def process(self, inputs, outputs) -> None:

        agg = inputs.get_value_obj("distribution").data
        col = inputs.get_value_obj("column").data
        
        table_obj = inputs.get_value_data("table")
        arrow_table = table_obj.arrow_table
        sources = arrow_table.to_pandas()
        
        sources["date"] = pd.to_datetime(sources["date"])

        if agg == 'month':
            query = f"SELECT strptime(concat(month, '/', year), '%m/%Y') as date, {col} as publication_name, count FROM (SELECT YEAR(date) as year, MONTH(date) as month, {col}, count(*) as count FROM sources GROUP BY {col}, YEAR(date), MONTH(date))"
    
        elif agg == 'year':
            query = f"SELECT strptime(year, '%Y') as date, {col} as publication_name, count FROM (SELECT YEAR(date) as year, {col}, count(*) as count FROM sources GROUP BY {col}, YEAR(date))"
        
        elif agg == 'day':
            query = f"SELECT strptime(concat('01/', month, '/', year), '%d/%m/%Y') as date, {col} as publication_name, count FROM (SELECT YEAR(date) as year, MONTH(date) as month, {col}, count(*) as count FROM sources GROUP BY {col}, YEAR(date), MONTH(date), DAY(date))"
        
        queried_df = duckdb.query(query).df()

        queried_df = queried_df.astype(str)

        viz_data = queried_df.to_dict('records')
        
        outputs.set_value("viz_data", viz_data)
