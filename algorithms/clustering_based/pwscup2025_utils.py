import numpy as np


def get_num_ranges(data, qi_index, is_cat):
    num_ranges = {}

    columns = list(zip(*data))
    for pos, idx in enumerate(qi_index):
        if is_cat[pos] == True:
            continue
        num_ranges[str(idx)] = max(columns[idx]) - min(columns[idx])

    return num_ranges


def get_distance(r, record, qi_index, is_cat, num_ranges):
    distances = []

    for pos, idx in enumerate(qi_index):
        if is_cat[pos] == True:
            distances.append(1 if r[idx] != record[idx] else 0)
        else:
            distances.append(abs(r[idx] - record[idx]) / num_ranges[str(idx)])

    return sum(distances)


def get_information_loss(record, cluster, qi_index, is_cat, num_ranges):
    information_losses = []
    if record == None:
        size = len(cluster)
        columns = list(zip(*cluster))
    else:
        size = len(cluster) + 1
        columns = list(zip(*(cluster + [record])))

    for pos, idx in enumerate(qi_index):
        if is_cat[pos] == True:
            information_losses.append(1 if len(set(columns[idx])) > 1 else 0)
        else:
            information_losses.append(
                (max(columns[idx]) - min(columns[idx]))
                / num_ranges[str(idx)]
            )

    return size * sum(information_losses)


