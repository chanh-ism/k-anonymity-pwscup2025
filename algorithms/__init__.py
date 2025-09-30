from .mondrian import classic_mondrian_anonymize, pwscup2025_mondrian_anonymize
from .clustering_based import cluster_based_anonymize

def k_anonymize(anon_params):

    if anon_params["name"] == "pwscup2025_mondrian":
        return pwscup2025_mondrian_anonymize(
            anon_params["value"], 
            anon_params["data"], 
            anon_params["qi_index"], 
            anon_params['mapping_dict'],
            anon_params['is_cat'],
            anon_params['is_int'],
        )

    if anon_params["name"] == "classic_mondrian":
        return classic_mondrian_anonymize(
            anon_params["value"], 
            anon_params["data"], 
            anon_params["qi_index"], 
            anon_params['mapping_dict'],
            anon_params['is_cat'],
            relax=False)

    elif anon_params["name"] == "cluster":
        return cluster_based_anonymize(
            anon_params["value"], 
            anon_params["att_trees"], 
            anon_params["data"], 
            anon_params["qi_index"], 
            anon_params["sa_index"], 
            type_alg='kmember')
