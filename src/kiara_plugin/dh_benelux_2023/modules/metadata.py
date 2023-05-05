# -*- coding: utf-8 -*-

from pydantic import Field

from kiara.api import KiaraModule, KiaraModuleConfig, ValueMap, ValueMapSchema
from kiara.exceptions import KiaraProcessingException
import re
import pandas as pd

class GetLCCNMetadataConfig(KiaraModuleConfig):

    separator: str = Field(
        description="The seperator between the two strings.", default=" - "
    )


class GetLCCNMetadata(KiaraModule):
    """

    """

    # _config_cls = ExampleModuleConfig
    _module_type_name = "dh_benelux_2023.get_lccn_metadata"

    def create_inputs_schema(self):
        
        return {
            "table_input": {
                "type": "table",
                "doc": "The corpus for which we want to extract metadata from file names."
            },
            "column_name": {
                "type": "string",
                "doc": "The column containing metadata. In order to work, file names need to comply with LCCN pattern '/sn86069873/1900-01-05/' containing publication reference and date."
            }
        }

    def create_outputs_schema(self):
        return {
            "table_output": {
                "type": "table",
                "doc": "Augmented table containing extracted metadata."
            },
            "publications_ref": {
                "type": "list",
                "doc": "List of unique publications refs in table."
             },
        }

    def process(self, inputs, outputs) -> None:

        table_obj = inputs.get_value_obj("table_input")
        column_name = inputs.get_value_obj("column_name").data

        df = table_obj.data.to_pandas()

         # get publication ref from file name
        def get_ref(file):
            ref_match = re.findall(r'(\w+\d+)_\d{4}-\d{2}-\d{2}_',file)
            if not ref_match:
                raise KiaraProcessingException(f"Can't process corpus, invalid format for file name: {file}")
            return ref_match[0]

        # get date from file name
        def get_date(file):
            date_match = re.findall(r'_(\d{4}-\d{2}-\d{2})_',file)
            if not date_match:
                raise KiaraProcessingException(f"Can't process corpus, invalid format for file name: {file}")
            return date_match[0]
        
        df['date'] = df['file_name'].apply(lambda x: get_date(x))

        df['publication'] = df[column_name].apply(lambda x: get_ref(x))
        
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(by='date')

        publications = df['publication'].unique().tolist()
        # counts = [df['publication'].value_counts().index.to_list(),df['publication'].value_counts().to_list()]

        outputs.set_value("table_output", df)
        # unique publications references useful at the next step to map publications references with publications names
        outputs.set_value("publications_ref", publications)
        # outputs.set_value("publications_count", counts)