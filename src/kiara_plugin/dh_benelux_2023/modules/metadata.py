# -*- coding: utf-8 -*-
from kiara.api import KiaraModule
from kiara.exceptions import KiaraProcessingException
import re
import pandas as pd

KIARA_METADATA = {
    "authors": [
        {"name": "Lorella Viola", "email": "lorella.viola@uni.lu"},
        {"name": "Mariella De Crouy", "email": "mariella.decrouychanel@uni.lu"},
    ]
}



class GetLCCNMetadata(KiaraModule):
    """
    This module will get metadata from strings that comply with LCCN pattern: '/sn86069873/1900-01-05/' to get the publication references and the dates and add those informations as two new columns.
    In addition, if a mapping scheme is provided between publication references and publication names, it will add a column with the publication names.
    Such a map is provided in the form of a list of lists with publication references and publication names in the same order.
    Here is an example of how it should look:
    [["2012271201","sn85054967","sn93053873"],["Cronaca_Sovversiva","Il_Patriota","L'Indipendente"]]
    """

    # _config_cls = ExampleModuleConfig
    _module_type_name = "get_lccn_metadata"

    def create_inputs_schema(self):
        
        return {
            "table_input": {
                "type": "table",
                "doc": "The corpus for which we want to get metadata from file names.",
            },
            "column_name": {
                "type": "string",
                "doc": "The column containing metadata. In order to work, file names need to comply with LCCN pattern '/sn86069873/1900-01-05/' containing publication reference and date."
            },
            "map": {
                "type": "list",
                "doc": "List of lists of unique publications references and publication names in the collection provided in the same order.",
                "optional": True,
            }
        }

    def create_outputs_schema(self):
        return {
            "table_output": {
                "type": "table",
                "doc": "Augmented table containing extracted metadata."
            }
        }

    def process(self, inputs, outputs) -> None:

        table_obj = inputs.get_value_obj("table_input")
        column_name = inputs.get_value_obj("column_name").data
        try:
            pub_refs = inputs.get_value_obj("map").data[0]
            pub_names = inputs.get_value_obj("map").data[1]
        except:
            pass

        sources = table_obj.data.to_pandas()

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
        
        # add date column
        sources['date'] = sources['file_name'].apply(lambda x: get_date(x))

        # add publication reference column
        sources['pub_ref'] = sources[column_name].apply(lambda x: get_ref(x))

        try:
            # if map with publications names available, add publication names
            sources['pub_name'] = sources['pub_ref'].replace(pub_refs, pub_names)
        except:
            pass

       
        outputs.set_value("table_output", sources)


class GetTextStats(KiaraModule):
    """
    This module will add columns with words and characters count to a table containing text content.
    """

    # _config_cls = ExampleModuleConfig
    _module_type_name = "get_text_stats"

    def create_inputs_schema(self):
        
        return {
            "table_input": {
                "type": "table",
                "doc": "The corpus for which we want to add words and characters count.",
            },
            "column_name": {
                "type": "string",
                "doc": "The column containing the text for which we want the count."
            }
        }

    def create_outputs_schema(self):
        return {
            "table_output": {
                "type": "table",
                "doc": "Augmented table containing words and characters count."
            }
        }

    def process(self, inputs, outputs) -> None:

        table_obj = inputs.get_value_obj("table_input")
        column_name = inputs.get_value_obj("column_name").data

        sources = table_obj.data.to_pandas()
        
        sources['chars_count'] = sources[column_name].apply(lambda x: len(x))
        sources['words_count'] = sources[column_name].apply(lambda x: len(x.split()))
       
        outputs.set_value("table_output", sources)