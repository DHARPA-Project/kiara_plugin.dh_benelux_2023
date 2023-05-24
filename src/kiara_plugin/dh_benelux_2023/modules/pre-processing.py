# -*- coding: utf-8 -*-
from kiara.api import KiaraModule
from kiara_plugin.tabular.models.array import KiaraArray
import pyarrow as pa
import gensim

KIARA_METADATA = {
    "authors": [
        {"name": "Lorella Viola", "email": "lorella.viola@uni.lu"},
        {"name": "Mariella De Crouy", "email": "mariella.decrouychanel@uni.lu"},
    ]
}

class GetBigrams(KiaraModule):
    """
    This module computes bigrams and/or trigrams.
    """

    # _config_cls = ExampleModuleConfig
    _module_type_name = "get_bigrams"

    def create_inputs_schema(self):
        
        return {
            "tokens": {
                "type": "array",
                "doc": "The array containing the tokens."
            },
            "bigrams_threshold": {
                "type": "integer",
                "doc": "Score threshold for forming the phrases (a higher score means fewer phrases)"
            },
            "bigrams_min_count": {
                "type": "integer",
                "doc": "Ignore phrases with total collected count lower than this value."
            },
            # "trigrams": {
            #     "type": "boolean",
            #     "doc": "Whether to compute trigrams in addition to bigrams.",
            # },
            # "trigrams_threshold": {
            #     "type": "integer",
            #     "doc": "Score threshold for forming the phrases (a higher score means fewer phrases)"
            # },
            # "trigrams_min_count": {
            #     "type": "integer",
            #     "doc": "Ignore phrases with total collected count lower than this value."
            # },
        }

    def create_outputs_schema(self):
        return {
            "tokens_array": {
                "type": "array",
                "doc": "The modified tokens with bigrams."
            }
        }

    def process(self, inputs, outputs) -> None:

        tokens_array: KiaraArray = inputs.get_value_data("tokens")
        bigrams_threshold : int = inputs.get_value_data("bigrams_threshold")
        bigrams_min_count : int = inputs.get_value_data("bigrams_min_count")
        # trigrams: bool = inputs.get_value_data("trigrams")
        # trigrams_threshold : int = inputs.get_value_data("trigrams_threshold")
        # trigrams_min_count : int = inputs.get_value_data("trigrams_min_count")

        tokens = tokens_array.arrow_array.to_pylist()

        bigram = gensim.models.Phrases(tokens, min_count=bigrams_min_count, threshold=bigrams_threshold)
        bigram_mod = gensim.models.phrases.Phraser(bigram)

        bigram_list = [bigram_mod[doc] for doc in tokens]

    
        result_array = pa.array(bigram_list)

        outputs.set_value("tokens_array", result_array)
      