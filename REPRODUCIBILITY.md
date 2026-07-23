# Reproducibility specification

## Source of truth

`data/raw/data_extraction_sheet_template.xlsx` is treated as immutable input. The pipeline reads the `Worksheet` sheet (or the first sheet if that name is absent), removes only entirely blank rows in memory, and validates the required column names. It never overwrites the workbook.

The canonical publication year is `publication year`. `Year` is used only when the canonical field is missing. Conflicts between the two fields are reported.

## Unit of analysis

The unit is one workbook row (one coded review record). Normalized titles are used only to report likely duplicates; rows are not silently deleted. Accordingly, thematic counts are record-level counts unless a table explicitly reports unique normalized titles.

## Classification

Thematic categories and their regular expressions are defined in `analysis/category_rules.json`. Each family is applied only to its documented source fields:

- concepts: title, `RQ1_Related_Constructs`, and `RQ1_Core_Definition`;
- hierarchy: `RQ1_Hierarchy_Level`;
- model type, inputs, coupling, temporality, modeling family, standards, and computing: RQ2 fields;
- validation, fidelity, reproducibility, and quality: RQ3 fields;
- domain, function, maturity, benefits, and limitations: RQ4 fields;
- ethical, technical, data, interoperability, and organizational barriers: RQ5 fields; and
- research, standardization, technology, and governance priorities: RQ6 fields.

Common null and negation phrases such as “not reported,” “not addressed,” “none,” and “unclear” are removed before pattern matching. A theme contributes at most one count per record, but themes are intentionally non-mutually exclusive.

Primary terminology is the first matching construct found, in order, in the title, related-construct field, and core definition. For multi-level hierarchy entries, Figure 2 uses the highest coded biological level. Coupling/temporality is encoded as 0 = static/manual, 1 = one-way, 2 = dynamic/continuous, and 3 = bidirectional.

## Figures

1. **PRISMA flow:** generated from `data/config/prisma_counts.csv` and arithmetically checked.
2. **Digital patient spectrum:** hierarchy versus temporal/coupling score; bubble area uses reported included-paper counts and is capped for readability. Records sharing the same categorical combination are deterministically packed within that cell (up to five columns by three rows), with offsets bounded away from the category borders. This reduces overplotting without changing any record's hierarchy or coupling classification.
3. **Computational pipeline:** record-level counts for inputs, model families, and functions.
4. **Validation landscape:** non-mutually exclusive validation and maturity counts.
5. **Application landscape:** domain-function co-occurrence heatmap.
6. **Barrier roadmap:** non-mutually exclusive barrier-category counts.

Every figure is exported as vector PDF for the manuscript and PNG for convenient review.

All figures inherit a centralized publication theme from `apply_publication_style()` in
`analysis/generate_outputs.py`. The theme uses embedded TrueType text, a
colorblind-conscious palette, restrained grid lines, consistent titles and notes, and
white backgrounds suitable for print. PNG review files are exported at 300 dpi; PDF
files remain vector-based. Bar figures report both record counts and percentages, while
the underlying classifications and numerical values are unchanged by the visual styling.

## Tables

Tables 1--7 are written both as CSV (easy to audit) and LaTeX (included directly by the manuscript). The Results section imports generated snippets rather than repeating hard-coded counts.

## Quality gates

`python analysis/generate_outputs.py --check` verifies input schema and required output files. `--strict` additionally returns a non-zero exit code for any QA warning; it currently does so because the supplied PRISMA counts are inconsistent.

`python -m unittest discover -s tests` checks text normalization, category matching, LaTeX escaping, number parsing, and PRISMA arithmetic.
