# +
import argparse

import pandas as pd
# -

from algorithms import k_anonymize
from utils.data import numberize_categories

parser = argparse.ArgumentParser(
    "anonymize-pws", formatter_class=argparse.RawTextHelpFormatter
)
parser.add_argument(
    "--method", type=str, default="pwscup2025_mondrian", help="K-Anonymity Method"
)
parser.add_argument("--k", type=int, default=5, help="K-Anonymity")
parser.add_argument(
    "-i", "--input", type=str, required=True, help="Input CSV file path (優先)"
)
parser.add_argument(
    "-o", "--output", type=str, default=None, help="Output CSV file path (優先)"
)
parser.add_argument(
    "-f",
    "--flags-ignored",
    type=str,
    default=["asthma", "stroke", "obesity", "depression"],
    nargs="*",
    help="""
    IGNORED (i.e., NOT anonymized) flag(s) (asthma, stroke, obesity, depression) 
    (Default: [], i.e., no flags are ignored, means all flags are NOT anonymized).
    Example: To ignore asthma and stroke, run:
    python anonymize-pws.py --input=B22_1.csv --flags-ignored asthma stroke
    
    To ANONYMIZE all flags (NOT IGNORING), call this argument without any input.
    Example: python anonymize-pws.py --input=B22_1.csv --flags-ignored
    """,
)


class Anonymizer:
    def __init__(self, args):
        self.method = args.method
        assert self.method in [
            "pwscup2025_mondrian",
            "classic_mondrian",
            "kmember",
            "oka",
        ]

        # ↓ 追加: 入出力パスの上書き
        self.input_csv = args.input
        self.output_csv = args.output

        assert args.k > 1
        self.k = args.k

        self.flags = ["asthma", "stroke", "obesity", "depression"]

        self.anonymized_flags_idx = [10, 11, 12, 13]
        self.ignore_flags = []
        for flag in args.flags_ignored:
            assert flag in self.flags, f"Flag '{flag}' not found!"
            self.ignore_flags.append(flag)
            self.anonymized_flags_idx.remove(10 + self.flags.index(flag))

        if self.ignore_flags != []:
            print(f"To be ignored (NOT anonymized) flags: {self.ignore_flags}")
        if self.anonymized_flags_idx != []:
            print(
                f"To be anonymized flags: {[self.flags[i-10] for i in self.anonymized_flags_idx]}"
            )

    def anonymize(self):
        data = pd.read_csv(self.input_csv)
        ATT_NAMES = list(data.columns)

        # index of quasi-identifiers:
        # PWS-CUP 2025: all except NOT-ignored flags
        QI_INDEX = list(range(0, 10)) + self.anonymized_flags_idx + list(range(14, 18))

        # whether a quasi-identifier is categorical data
        # PWS-CUP 2025: (GENDER, RACE, ETHNICITY) and the flags
        IS_CATEGORICAL = [False] * len(QI_INDEX)
        for idx in [0, 2, 3] + [QI_INDEX.index(idx) for idx in self.anonymized_flags_idx]:
            IS_CATEGORICAL[idx] = True

        # PWS-CUP 2025: quasi-identifers that are required to be integer
        IS_INT = [1, 4, 5, 6, 7, 8, 9]

        SA_INDEX = [index for index in range(len(ATT_NAMES)) if index not in QI_INDEX]

        anon_params = {
            "name": self.method,
            "data": data.values.tolist(),
            "value": self.k,
            "qi_index": QI_INDEX,
            "sa_index": SA_INDEX,
            "is_cat": IS_CATEGORICAL,
            "is_int": IS_INT,
        }

        if "mondrian" in self.method:
            mapping_dict, raw_data = numberize_categories(
                data.values.tolist(), QI_INDEX, SA_INDEX, IS_CATEGORICAL
            )
            anon_params["mapping_dict"] = mapping_dict
            anon_params["data"] = raw_data

        anon_data, runtime = k_anonymize(anon_params)

        # Write anonymized table
        if anon_data is not None:
            if self.output_csv is not None:
                output_path = self.output_csv
            else:
                ignore_flags_txt = (
                    "_".join(self.ignore_flags)
                    if len(self.ignore_flags) < 4
                    else "all_flags"
                )
                ignore_txt = (
                    "-ignore_None"
                    if ignore_flags_txt == ""
                    else f"-ignore_{ignore_flags_txt}"
                )
                output_path = f'{self.input_csv.split(".csv")[0]}-{self.method}-k{self.k}{ignore_txt}.csv'
            pd.DataFrame(anon_data, columns=ATT_NAMES).to_csv(output_path, index=False)
            print(f"Saved output to '{output_path}'.")


def main(args):
    anonymizer = Anonymizer(args)
    anonymizer.anonymize()


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
