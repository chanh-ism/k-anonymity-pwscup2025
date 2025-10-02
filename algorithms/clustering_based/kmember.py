# +
import random
import time

import numpy as np
from tqdm.auto import tqdm

from .pwscup2025_utils import get_distance, get_information_loss, get_num_ranges


# -

def find_furthest_record_from_r(r, data, qi_index, is_cat, num_ranges):
    max_distance = 0
    furthest_idx = None
    furthest_r = None

    for idx, record in enumerate(data):
        this_distance = get_distance(r, record, qi_index, is_cat, num_ranges)
        if this_distance > max_distance:
            max_distance = this_distance
            furthest_r = record
            furthest_idx = idx

    return (furthest_r, furthest_idx)


def find_best_record(data, cluster, qi_index, is_cat, num_ranges):
    min_information_loss = np.inf
    best_idx = None
    best_r = None

    for idx, record in enumerate(data):
        this_information_loss = get_information_loss(
            record, cluster, qi_index, is_cat, num_ranges
        )
        if this_information_loss < min_information_loss:
            min_information_loss = this_information_loss
            best_r = record
            best_idx = idx

    return (best_r, best_idx, min_information_loss)


def find_best_cluster(clusters, r, qi_index, is_cat, num_ranges):
    min_information_loss = np.inf
    best_idx = None

    for idx, cluster in enumerate(clusters):
        this_information_loss = get_information_loss(
            r, cluster, qi_index, is_cat, num_ranges
        )
        if this_information_loss < min_information_loss:
            min_information_loss = this_information_loss
            best_idx = idx

    return (bext_idx, min_information_loss)


def do_clustering_kmember(data, k, qi_index, is_cat):
    num_ranges = get_num_ranges(data, qi_index, is_cat)
    clusters = []
    information_losses = []
    r_i = None

    progress_bar = tqdm(
        total=len(data),
        desc="   Clustering Progress",
        bar_format="{l_bar}{bar:100}|{n_fmt}/{total_fmt} [{elapsed}]",
    )

    while len(data) >= k:
        if r_i is None:
            r_i_idx = random.randrange(len(data))
            r_i = data[r_i_idx]
        else:
            r_i, r_i_idx = find_furthest_record_from_r(
                r_i, data, qi_index, is_cat, num_ranges
            )
        data.pop(r_i_idx)
        this_cluster = [r_i]
        this_information_loss = None

        while len(this_cluster) < k:
            r_j, r_j_idx, this_information_loss = find_best_record(
                data, this_cluster, qi_index, is_cat, num_ranges
            )
            data.pop(r_j_idx)
            this_cluster.append(r_j)

        information_losses.append(this_information_loss)
        clusters.append(this_cluster)
        progress_bar.update(k)

    for r in data:
        cluster_idx, information_loss = find_best_cluster(
            clusters, r, qi_index, is_cat, num_ranges
        )
        information_losses[cluster_idx] = information_loss
        clusters[cluster_idx].append(r)
        progress_bar.update(1)

    return (clusters, information_losses)


def pwscup2025_kmember_anon(data, k, qi_index, is_cat, is_int):
    result = []
    start_time = time.time()
    clusters, information_losses = do_clustering_kmember(
        data.copy(), k, qi_index, is_cat
    )

    progress_bar = tqdm(
        total=len(clusters),
        desc="Anonymization Progress",
        bar_format="{l_bar}{bar:100}|{n_fmt}/{total_fmt} [{elapsed}]",
    )

    for cluster in clusters:
        columns = list(zip(*cluster))
        for pos, idx in enumerate(qi_index):
            anon_value = None
            if is_cat[pos] == True:
                anon_value = max(columns[idx], key=columns[idx].count)
            else:
                anon_value = sum(columns[idx]) / len(columns[idx])
                if idx in is_int:
                    anon_value = round(anon_value)
            columns[idx] = list(map(lambda x: anon_value, columns[idx]))

        result.extend(list(zip(*columns)))
        progress_bar.update()

    progress_bar.close()
    print(f"Information Loss: {sum(information_losses)}")
    rtime = float(time.time() - start_time)
    return (result, rtime)
