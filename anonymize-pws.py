# +
import argparse

import pandas as pd
# -

from algorithms import k_anonymize
from utils.data import numberize_categories

parser = argparse.ArgumentParser("K-Anonymize")
parser.add_argument(
    "--method", type=str, default="pwscup2025_mondrian", help="K-Anonymity Method"
)
parser.add_argument("--k", type=int, default=5, help="K-Anonymity")
parser.add_argument(
    "-i", "--input", type=str, default=None, help="Input CSV file path (優先)"
)
parser.add_argument(
    "-o", "--out", type=str, default=None, help="Output CSV file path (優先)"
)


class Anonymizer:
    def __init__(self, args):
        self.method = args.method
        assert self.method in [
            "pwscup2025_mondrian",
        ]

        assert args.input is not None
        # ↓ 追加: 入出力パスの上書き
        self.input_csv = args.input
        self.output_csv = args.out

        self.k = args.k

    def anonymize(self):
        data = pd.read_csv(self.input_csv)
        ATT_NAMES = list(data.columns)

        # index of quasi-identifiers:
        # PWS-CUP 2025: all except [10, 11, 12, 13] (asthma, stroke, obesity, depression)
        QI_INDEX = list(range(0, 10)) + list(range(14, 18))

        # whether a quasi-identifier is categorical data
        # PWS-CUP 2025: (GENDER, RACE, ETHNICITY)
        IS_CATEGORICAL = [True, False, True, True] + ([False] * 10)

        # PWS-CUP 2025: quasi-identifers that are required to be integer
        IS_INT = [1, 4, 5, 6, 7, 8, 9]

        SA_INDEX = [index for index in range(len(ATT_NAMES)) if index not in QI_INDEX]

        anon_params = {
            "name": self.method,
            "value": self.k,
            "qi_index": QI_INDEX,
            "sa_index": SA_INDEX,
            "is_cat": IS_CATEGORICAL,
            "is_int": IS_INT,
        }

        mapping_dict, raw_data = numberize_categories(
            data.values.tolist(), QI_INDEX, SA_INDEX, IS_CATEGORICAL
        )
        anon_params["mapping_dict"] = mapping_dict
        anon_params["data"] = raw_data

        anon_data, runtime = k_anonymize(anon_params)

        # Write anonymized table
        if anon_data is not None:
            output_path = (
                self.output_csv
                if self.output_csv is not None
                else f"{self.input_csv.split(".csv")[0]}-anon-k{self.k}.csv"
            )
            pd.DataFrame(anon_data, columns=ATT_NAMES).to_csv(output_path, index=False)


def main(args):
    anonymizer = Anonymizer(args)
    anonymizer.anonymize()


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
