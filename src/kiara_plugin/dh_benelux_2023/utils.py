# -*- coding: utf-8 -*-

def augment_lineage_data(item,kiara):

    graph = item.lineage.module_graph
    
    nodes = graph.nodes.data()
    augmented_nodes = dict()


    def get_info(node):
            # all this is terribly inefficient
            
            if node[1]["node_type"] == "operation":
                
                result = kiara.retrieve_module_type_info(node[1]["module_type"]).dict()
            
            elif node[1]["node_type"] == "value":
                
                value_id = node[0][6:]
                
                v = kiara.get_value(value_id)
                if v.is_set:
                    render_result = kiara.render_value(value=v, target_format="string").rendered
                    
                else:
                    render_result = "None"

                result = {
                    "preview": render_result
                }
            
            return result

    for idx, node in enumerate(nodes):
            node_dict = {
                "id": node[0],
                "desc": node[1],
                "parentIds": [pred for pred in graph.predecessors(node[0])],
                "info": get_info(node)
            }
            augmented_nodes[idx] = node_dict

    return augmented_nodes