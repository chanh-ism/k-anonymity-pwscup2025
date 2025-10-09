# Data anonymization using $k$-Anonymity for PWS Cup 2025
This repo is a fork from https://github.com/kaylode/k-anonymity.

It has been modified for the purpose of PWS Cup 2025.

## Implemented $k$-Anonymity Methods
- Classic Mondrian [[1]](#references): For reference only. Not applicable for PWS Cup 2025.
- <u>Modified</u> Classic Mondrian (PWSCup2025 Mondrian): A modification of Classic Mondrian:
  - For numerical attributes: use _mean_ value instead of min-max range, and
  - For categorical attributes: use _mode_ value instead of summarization.
- Clustering-based:
  - $k$-member [[2]](#references) (very slow)
  - One-Pass K-Means Algorithm (OKA) [[3]](#references) 

## Executing
To perform anonymization on a dataset, run:
```sh
python anonymize-pws.py --method <model_type> --k <k-anonymity> --input <input_csv_path> --output <output_csv_path> --flags-ignored <flags_to_be_ignore>
```
- `model_type`: [`pwscup2025_mondrian` (default) | `classic_mondrian` | `oka` | `kmember` ]
- `k-anonymity`: $k > 1$ (default: $5$)
- `input_csv_path`: path to input csv file
- `output_csv_path`: (Optional) path to output csv file (default: same directory as `input`)
- `flags_to_be_ignore`: (Optional) flag(s) [`asthma`, `stroke`, `obesity`, `depression`] to be IGNORED (i.e., NOT anonymized)\
(Default: no flags are ignored, means all flags are NOT anonymized)

__Example 1__: To perform Modified Classic Mondrian (default) with $k=5$ (default) on `B22_1.csv`, run
```sh
python anonymize-pws.py --input B22_1.csv
```
The result is stored in `B22_1-pwscup2025_mondrian-k5.csv` by default.

__Example 2__: To perform OKA with $k=2$ on `B22_1.csv`, run
```sh
python anonymize-pws.py --input B22_1.csv --method oka --k 2
```
The result is stored in `B22_1-oka-k2.csv` by default.

__Example 3__: To perform OKA with $k=5$ (default) on `B22_1.csv` and ignoring `asthma` and `stroke` flags, run
```sh
python anonymize-pws.py --input B22_1.csv --method oka --flags-ignored asthma stroke
```
The result is stored in `B22_1-oka-k5-ignore_asthma_stroke.csv` by default.

## References
[1] LeFevre, Kristen, David J. DeWitt, and Raghu Ramakrishnan. "Mondrian multidimensional k-anonymity." Proceedings of the 22nd International conference on data engineering (ICDE'06). IEEE, 2006. https://doi.org/10.1109/ICDE.2006.101.

[2] Byun, JW., Kamra, A., Bertino, E., Li, N. "Efficient k-Anonymization Using Clustering Techniques." Proceedings of the International conference on database systems for advanced applications (DASFAA 2007). Lecture Notes in Computer Science, vol 4443. Springer, 2007. https://doi.org/10.1007/978-3-540-71703-4_18.

[3] Lin, Jun-Lin, and Meng-Cheng Wei. "An efficient clustering method for k-anonymization." Proceedings of the 2008 international workshop on Privacy and anonymity in information society. ACM, 2008. https://doi.org/10.1145/1379287.1379297.
