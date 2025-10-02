# +
import random
import time

from tqdm.auto import tqdm

from .pwscup2025_utils import get_distance, get_information_loss, get_num_ranges
# -

IS_CAT = []
QI_INDEX = []
NUM_RANGES = {}


class OKA_Cluster(object):
    def __init__(self, first_record):
        self.member = [first_record]
        self.centroid = first_record

    def add(self, record):
        self.member.append(record)
        self.__update_centroid()

    def remove(self, idx: int):
        assert len(self.member) > 0
        self.member.pop(idx)
        self.__update_centroid()

    def distance(self, record):
        return len(self.member) * get_distance(
            record,
            self.centroid,
            QI_INDEX,
            IS_CAT,
            NUM_RANGES,
        )

    def sort_by_distance(self):
        self.member.sort(
            key=lambda record: get_distance(
                record,
                self.centroid,
                QI_INDEX,
                IS_CAT,
                NUM_RANGES,
            )
        )

    def pop_tops(self, count: int):
        assert len(self.member) >= count

        results = []
        for idx in range(count):
            results.append(self.member.pop(idx))

        self.__update_centroid()
        return results

    def __update_centroid(self):
        if len(self.member) == 0:
            self.centroid = None
        elif len(self.member) == 1:
            self.centroid = self.member[0]
        else:
            centroid = []
            for idx, col in enumerate(zip(*self.member)):
                if idx not in QI_INDEX:
                    centroid.append(-1)
                elif IS_CAT[QI_INDEX.index(idx)] == True:
                    centroid.append(max(col, key=col.count))
                else:
                    centroid.append(sum(col) / len(col))
            self.centroid = centroid

    def __getitem__(self, item):
        return self.member[item]

    def __len__(self):
        return len(self.member)


def find_best_cluster(record, clusters):
    min_distance = float("inf")
    best_idx = None

    for idx, cluster in enumerate(clusters):
        distance = cluster.distance(record)
        if distance < min_distance:
            min_distance = distance
            best_idx = idx

    return best_idx


def do_clustering_oka(data, k):
    clusters = []

    for i in range(int(len(data) / k)):
        r_i_idx = random.randrange(len(data))
        clusters.append(OKA_Cluster(data.pop(r_i_idx)))

    clustering_progress_bar = tqdm(
        total=len(data),
        desc="   Clustering Progress",
        bar_format="{l_bar}{bar:100}|{n_fmt}/{total_fmt} [{elapsed}]",
    )

    # Clustering Stage
    while len(data) > 0:
        record = data.pop()
        best_cluster_idx = find_best_cluster(record, clusters)
        clusters[best_cluster_idx].add(record)
        clustering_progress_bar.update(1)

    clustering_progress_bar.close()
    adjustment_progress_bar = tqdm(
        total=(len(clusters) * 2),
        desc="   Adjustment Progress",
        bar_format="{l_bar}{bar:100}|{n_fmt}/{total_fmt} [{elapsed}]",
    )

    # Adjustment Stage
    adjusting_records = []
    less_than_k_clusters = []
    for cluster in clusters:
        if len(cluster) == k:
            adjustment_progress_bar.update(1)
            continue
        elif len(cluster) < k:
            less_than_k_clusters.append(cluster)
        else:
            cluster.sort_by_distance()
            adjusting_records.extend(cluster.pop_tops(len(cluster) - k))
        adjustment_progress_bar.update(1)

    while len(adjusting_records) > 0:
        record = adjusting_records.pop()
        if len(less_than_k_clusters) > 0:
            best_cluster_idx = find_best_cluster(record, less_than_k_clusters)
            less_than_k_clusters[best_cluster_idx].add(record)
            if len(less_than_k_clusters[best_cluster_idx]) >= k:
                less_than_k_clusters.pop(best_cluster_idx)
        else:
            best_cluster_idx = find_best_cluster(record, clusters)
            clusters[best_cluster_idx].add(record)

    adjustment_progress_bar.update(len(clusters))
    adjustment_progress_bar.close()

    return clusters


def init(data, qi_index, is_cat):
    global IS_CAT, QI_INDEX, NUM_RANGES
    QI_INDEX = qi_index
    IS_CAT = is_cat
    NUM_RANGES = get_num_ranges(data, qi_index, is_cat)


def pwscup2025_oka_anon(data, k, qi_index, is_cat, is_int):
    init(data, qi_index, is_cat)
    result = []
    start_time = time.time()
    clusters = do_clustering_oka(data.copy(), k)

    progress_bar = tqdm(
        total=len(clusters),
        desc="Anonymization Progress",
        bar_format="{l_bar}{bar:100}|{n_fmt}/{total_fmt} [{elapsed}]",
    )

    information_loss = 0
    for cluster in clusters:
        information_loss += get_information_loss(
            None, cluster.member, qi_index, is_cat, NUM_RANGES
        )
        columns = list(zip(*cluster.member))
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
        progress_bar.update(1)

    progress_bar.close()
    print(f"Information Loss: {information_loss}")
    rtime = float(time.time() - start_time)
    return (result, rtime)


