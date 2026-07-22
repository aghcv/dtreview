# Automated data-quality report

Input: `data/raw/data_extraction_sheet_template.xlsx`
Records: **64**
Unique normalized titles: **62**
Columns: **65** after derived fields

## Warnings

- 4 rows have conflicting 'publication year' and 'Year' values; the pipeline uses 'publication year' and falls back to 'Year' only when missing.
- PRISMA arithmetic mismatch for studies_included: source says 64, but preceding counts imply 59 (difference +5).

## Duplicate normalized titles

- ethica designing human digital twins a systematic review and proposed methodology (2 rows)
- toward a digital twin for arthroscopic knee surgery a systematic review (2 rows)

## Highest missingness among required source fields

| Field | Missing |
|---|---:|
| `RQ3_Quality_Tool_Applied` | 35.9% |
| `RQ3_Fidelity_Metric` | 25.0% |
| `RQ3_Validation_Data_Source` | 17.2% |
| `RQ2_Integration_Standards` | 12.5% |
| `RQ3_Validation_Method` | 12.5% |
| `RQ1_Related_Constructs` | 12.5% |
| `RQ6_Standardization_Recs` | 10.9% |
| `RQ2_Data_Coupling_Mode` | 7.8% |
| `RQ5_Technical_Barriers` | 7.8% |
| `RQ3_Reproducibility_Mentioned` | 6.2% |
| `RQ1_Core_Definition` | 6.2% |
| `RQ5_Interoperability_Gaps` | 6.2% |

## Interpretation

All thematic counts are record-level regex classifications defined in `analysis/category_rules.json`. They are intentionally non-mutually exclusive and should be reviewed after adjudication. The pipeline preserves the source workbook and writes row-level classifications to `generated/data/record_level_classification.csv` for audit.
