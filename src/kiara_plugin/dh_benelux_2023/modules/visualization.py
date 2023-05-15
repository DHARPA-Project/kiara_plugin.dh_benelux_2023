# -*- coding: utf-8 -*-

from pydantic import Field

from kiara.api import KiaraModule, KiaraModuleConfig, ValueMap, ValueMapSchema
from kiara.exceptions import KiaraProcessingException
import re
import pandas as pd

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
            }
        }

    def create_outputs_schema(self):
        return {
            "query": {
                "type": "string",
                "doc": "The query to pass to the sql query module."
            }
        }

    def process(self, inputs, outputs) -> None:

        agg = inputs.get_value_obj("distribution").data
        col = inputs.get_value_obj("column").data

        if agg == 'month':
            query = f"SELECT strptime(concat(month, '/', year), '%m/%Y') as date, {col} as publication_name, count FROM (SELECT YEAR(date) as year, MONTH(date) as month, {col}, count(*) as count FROM data GROUP BY {col}, YEAR(date), MONTH(date))"
    
        elif agg == 'year':
            query = f"SELECT strptime(year, '%Y') as date, {col} as publication_name, count FROM (SELECT YEAR(date) as year, {col}, count(*) as count FROM data GROUP BY {col}, YEAR(date))"
        
        elif agg == 'day':
            query = f"SELECT strptime(concat('01/', month, '/', year), '%d/%m/%Y') as date, {col} as publication_name, count FROM (SELECT YEAR(date) as year, MONTH(date) as month, {col}, count(*) as count FROM data GROUP BY {col}, YEAR(date), MONTH(date), DAY(date))"
        
        outputs.set_value("query", query)
