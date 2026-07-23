# Digital Patient Meta-Review

This repository keeps the review protocol, screening-adjudication protocol, extraction data, manuscript sources, and reproducible evidence synthesis in one version-controlled location. The root `main.tex` is ready to import into Overleaf and uses the supplied SAGE LaTeX template.

## Repository map

| Path | Purpose |
|---|---|
| `protocols/` | PROSPERO draft and CADIMA inconsistency-resolution protocol |
| `data/raw/` | Immutable extraction workbook used as the analytic source of truth |
| `data/config/` | Non-workbook inputs, currently PRISMA counts transcribed from the project flow diagram |
| `manuscript_source/` | Current Word draft, original template archive, and source figure templates |
| `analysis/` | Executable synthesis pipeline and transparent classification rules |
| `generated/` | Regenerated LaTeX tables, CSV tables, vector/raster figures, plotting data, and QA report |
| `sections/` | Human-edited manuscript narrative included by `main.tex` |
| `template_original/` | Unmodified contents of the supplied SAGE template archive |

## Reproduce all tables and figures

Python 3.10 or newer is recommended.

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python analysis/generate_outputs.py --check
```

Or use:

```bash
make analysis
make test
```

The pipeline does not edit the extraction workbook. It writes only to `generated/` and produces:

- Tables 1--7 as both `.tex` and `.csv`;
- Figures 1--6 as vector `.pdf` and 300-dpi `.png` files in two publication variants: `single_portrait` for one-column placement and `double_landscape` for two-column placement;
- `generated/data/record_level_classification.csv` for audit;
- manuscript-ready result snippets; and
- `generated/data_quality_report.md`.

## Compile the Overleaf manuscript locally

With [Tectonic](https://tectonic-typesetting.github.io/) installed:

```bash
make manuscript
```

The resulting PDF is written to `build/main.pdf`. On Overleaf, set `main.tex` as the main document. The generated artifacts are committed, so Overleaf does not need a Python runtime.

## Editing workflow

1. Edit the extraction workbook in `data/raw/` or replace it with an adjudicated version using the same column names.
2. Update `analysis/category_rules.json` only when the team intentionally changes a classification rule.
3. Run `make analysis` and review the row-level classifications plus `generated/data_quality_report.md`.
4. Run `make manuscript` and inspect the compiled PDF.
5. Commit source changes and regenerated artifacts together.

## Important current QA item

The supplied PRISMA source reports 247 full-text reports assessed, exclusions of 179, 9, and 0, and 64 included studies. Those values do not reconcile: the exclusion counts imply 59 included studies, a difference of five. The pipeline preserves the source values, marks the draft PRISMA figure, and reports the mismatch. Reconcile these counts before journal submission.

See [REPRODUCIBILITY.md](REPRODUCIBILITY.md) for the analytical decisions and audit trail.
