#!/usr/bin/env python3
"""Regenerate manuscript tables, figures, data extracts, and QA reports.

The extraction workbook remains immutable. All derived artifacts are written to
``generated/`` so that every number in the manuscript can be traced back to a
record-level classification and an explicit regular-expression rule.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import textwrap
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("MPLCONFIGDIR", str(ROOT / ".cache" / "matplotlib"))
os.environ.setdefault("XDG_CACHE_HOME", str(ROOT / ".cache"))
Path(os.environ["MPLCONFIGDIR"]).mkdir(parents=True, exist_ok=True)

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch
import numpy as np
import pandas as pd


REQUIRED_COLUMNS = [
    "article id",
    "publication year",
    "title",
    "Number_of_Papers_Included",
    "RQ1_Core_Definition",
    "RQ1_Related_Constructs",
    "RQ1_Hierarchy_Level",
    "RQ2_Model_Type",
    "RQ2_Data_Sources_Used",
    "RQ2_Data_Coupling_Mode",
    "RQ2_Temporal_Orientation",
    "RQ2_Modeling_Approach",
    "RQ2_Integration_Standards",
    "RQ2_Computing_Infrastructure",
    "RQ3_Validation_Method",
    "RQ3_Validation_Data_Source",
    "RQ3_Fidelity_Metric",
    "RQ3_Reproducibility_Mentioned",
    "RQ3_Quality_Tool_Applied",
    "RQ4_Healthcare_Domain",
    "RQ4_Function_Category",
    "RQ4_Maturity_Level",
    "RQ4_Reported_Limitations",
    "RQ5_Ethical_Issues",
    "RQ5_Technical_Barriers",
    "RQ5_Data_Limitations",
    "RQ5_Interoperability_Gaps",
    "RQ5_Org_Barriers",
    "RQ6_Research_Priorities",
    "RQ6_Standardization_Recs",
]

NEGATION_PHRASES = re.compile(
    r"\b(?:not reported|not addressed|not mentioned|not specified|not systematically "
    r"reported|not formally reported|no specific|no formal|none reported|none|n/?a|unclear)\b",
    flags=re.IGNORECASE,
)

COLORS = [
    "#1F4E79",
    "#2A9D8F",
    "#E76F51",
    "#E9C46A",
    "#6D597A",
    "#457B9D",
    "#8AB17D",
    "#B56576",
    "#7F8C8D",
]


@dataclass
class AnalysisBundle:
    records: pd.DataFrame
    masks: dict[str, pd.DataFrame]
    counts: dict[str, pd.Series]
    year_counts: pd.Series
    warnings: list[str]


def normalize_text(value: object) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    text = str(value).lower().replace("\u2013", "-").replace("\u2014", "-")
    text = NEGATION_PHRASES.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def combined_text(df: pd.DataFrame, columns: Sequence[str]) -> pd.Series:
    parts = [df[column].map(normalize_text) for column in columns]
    return pd.concat(parts, axis=1).agg(" | ".join, axis=1)


def match_categories(
    df: pd.DataFrame, columns: Sequence[str], rules: Mapping[str, str]
) -> pd.DataFrame:
    searchable = combined_text(df, columns)
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="This pattern is interpreted as a regular expression, and has match groups",
            category=UserWarning,
        )
        return pd.DataFrame(
            {
                label: searchable.str.contains(pattern, regex=True, case=False, na=False)
                for label, pattern in rules.items()
            },
            index=df.index,
        )


def classify_primary_term(row: pd.Series) -> str:
    sources = [normalize_text(row.get("title", "")), normalize_text(row.get("RQ1_Related_Constructs", "")), normalize_text(row.get("RQ1_Core_Definition", ""))]
    ordered = [
        ("Patient digital twin", r"patient(?:'s)? digital twin"),
        ("Human digital twin", r"human digital twin|\bhdt\b"),
        ("In silico patient / trial", r"in[- ]?silico (?:patient|trial|clinical)"),
        ("Virtual patient", r"virtual patients?"),
        ("Synthetic patient", r"synthetic patients?"),
        ("Digital patient", r"digital patients?"),
        ("Digital twin", r"digital twins?|\bdt\b"),
    ]
    for source in sources:
        for label, pattern in ordered:
            if re.search(pattern, source, flags=re.IGNORECASE):
                return label
    return "Other / adjacent construct"


def classify_hierarchy(value: object) -> float:
    text = normalize_text(value)
    levels = [
        (6, r"system of systems|system[- ]?of[- ]?systems"),
        (5, r"population|cohort|public health"),
        (4, r"whole[- ]?body|body|person|patient level|individual"),
        (3, r"organ|system level|physiolog"),
        (2, r"tissue"),
        (1, r"cell|molecular"),
    ]
    matches = [score for score, pattern in levels if re.search(pattern, text)]
    return float(max(matches)) if matches else np.nan


def classify_coupling(row: pd.Series) -> float:
    coupling = normalize_text(row.get("RQ2_Data_Coupling_Mode", ""))
    temporal = normalize_text(row.get("RQ2_Temporal_Orientation", ""))
    text = f"{coupling} | {temporal}"
    if re.search(r"two[- ]?way|bi[- ]?directional|bidirectional", text):
        return 3.0
    if re.search(r"continuous|real[- ]?time|dynamic|time[- ]?dependent|longitudinal", text):
        return 2.0
    if re.search(r"one[- ]?way|uni[- ]?directional|unidirectional", text):
        return 1.0
    if re.search(r"static|manual|no coupling", text):
        return 0.0
    return np.nan


def first_matching_label(mask_row: pd.Series, fallback: str = "Other / mixed") -> str:
    for label, matched in mask_row.items():
        if bool(matched):
            return str(label)
    return fallback


def parse_first_number(value: object) -> float:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return np.nan
    match = re.search(r"\d[\d,]*", str(value))
    return float(match.group(0).replace(",", "")) if match else np.nan


def latex_escape(value: object) -> str:
    text = str(value)
    replacements = [
        ("\\", r"\textbackslash{}"),
        ("&", r"\&"),
        ("%", r"\%"),
        ("$", r"\$"),
        ("#", r"\#"),
        ("_", r"\_"),
        ("{", r"\{"),
        ("}", r"\}"),
        ("~", r"\textasciitilde{}"),
        ("^", r"\textasciicircum{}"),
    ]
    for source, target in replacements:
        text = text.replace(source, target)
    return text


def semicolon_counts(series: pd.Series, top_n: int | None = None) -> str:
    ranked = series.sort_values(ascending=False)
    if top_n is not None:
        ranked = ranked.head(top_n)
    return "; ".join(f"{label} ({int(value)})" for label, value in ranked.items())


def write_latex_table(
    output: Path,
    caption: str,
    label: str,
    headers: Sequence[str],
    rows: Sequence[Sequence[object]],
) -> None:
    if len(headers) == 2:
        column_spec = r"@{}>{\raggedright\arraybackslash}p{0.30\textwidth}Y@{}"
    elif len(headers) == 3:
        column_spec = r"@{}>{\raggedright\arraybackslash}p{0.20\textwidth}YY@{}"
    else:
        column_spec = "@{}" + "X" * len(headers) + "@{}"
    lines = [
        r"\begin{table*}[htbp]",
        r"\centering",
        r"\small",
        rf"\caption{{{latex_escape(caption)}}}",
        rf"\label{{{label}}}",
        rf"\begin{{tabularx}}{{\textwidth}}{{{column_spec}}}",
        r"\toprule",
        " & ".join(rf"\textbf{{{latex_escape(header)}}}" for header in headers) + r" \\",
        r"\midrule",
    ]
    for row in rows:
        lines.append(" & ".join(latex_escape(cell) for cell in row) + r" \\")
    lines.extend([r"\bottomrule", r"\end{tabularx}", r"\end{table*}", ""])
    output.write_text("\n".join(lines), encoding="utf-8")


def load_rules(path: Path) -> dict[str, dict[str, str]]:
    return json.loads(path.read_text(encoding="utf-8"))


def read_workbook(path: Path) -> pd.DataFrame:
    workbook = pd.ExcelFile(path)
    sheet = "Worksheet" if "Worksheet" in workbook.sheet_names else workbook.sheet_names[0]
    df = pd.read_excel(path, sheet_name=sheet)
    df = df.dropna(how="all").reset_index(drop=True)
    missing = sorted(set(REQUIRED_COLUMNS) - set(df.columns))
    if missing:
        raise ValueError("Missing required workbook columns: " + ", ".join(missing))
    return df


def prisma_checks(counts: Mapping[str, int]) -> list[str]:
    checks = [
        (
            "screened",
            counts["identified_databases"]
            + counts["identified_registers"]
            - counts["duplicates_removed"]
            - counts["automation_removed"]
            - counts["other_removed"],
        ),
        ("reports_sought", counts["screened"] - counts["screening_excluded"]),
        ("reports_assessed", counts["reports_sought"] - counts["reports_not_retrieved"]),
        (
            "studies_included",
            counts["reports_assessed"]
            - counts["excluded_reason_1"]
            - counts["excluded_reason_2"]
            - counts["excluded_reason_3"],
        ),
    ]
    warnings: list[str] = []
    for key, expected in checks:
        actual = counts[key]
        if actual != expected:
            warnings.append(
                f"PRISMA arithmetic mismatch for {key}: source says {actual}, "
                f"but preceding counts imply {expected} (difference {actual - expected:+d})."
            )
    if counts["reports_included"] != counts["studies_included"]:
        warnings.append(
            "PRISMA source lists different counts for reports and studies included."
        )
    return warnings


def analyze(df: pd.DataFrame, rules: dict[str, dict[str, str]]) -> AnalysisBundle:
    masks = {
        "data_sources": match_categories(df, ["RQ2_Data_Sources_Used"], rules["data_sources"]),
        "model_families": match_categories(df, ["RQ2_Modeling_Approach"], rules["model_families"]),
        "standards": match_categories(df, ["RQ2_Integration_Standards"], rules["standards"]),
        "computing": match_categories(df, ["RQ2_Computing_Infrastructure"], rules["computing"]),
        "domains": match_categories(df, ["RQ4_Healthcare_Domain"], rules["domains"]),
        "functions": match_categories(df, ["RQ4_Function_Category"], rules["functions"]),
        "validation_methods": match_categories(
            df,
            ["RQ3_Validation_Method", "RQ3_Validation_Data_Source", "RQ3_Fidelity_Metric"],
            rules["validation_methods"],
        ),
        "maturity": match_categories(df, ["RQ4_Maturity_Level"], rules["maturity"]),
        "barriers": match_categories(
            df,
            [
                "RQ4_Reported_Limitations",
                "RQ5_Ethical_Issues",
                "RQ5_Technical_Barriers",
                "RQ5_Data_Limitations",
                "RQ5_Interoperability_Gaps",
                "RQ5_Org_Barriers",
            ],
            rules["barriers"],
        ),
    }
    counts = {name: mask.sum().astype(int) for name, mask in masks.items()}

    records = df.copy()
    publication_year = pd.to_numeric(records["publication year"], errors="coerce")
    alternate_year = pd.to_numeric(records.get("Year"), errors="coerce")
    records["analysis_year"] = publication_year.fillna(alternate_year)
    records["normalized_title"] = (
        records["title"].fillna("").astype(str).str.lower().str.replace(r"\W+", " ", regex=True).str.strip()
    )
    records["primary_term"] = records.apply(classify_primary_term, axis=1)
    records["hierarchy_score"] = records["RQ1_Hierarchy_Level"].map(classify_hierarchy)
    records["coupling_score"] = records.apply(classify_coupling, axis=1)
    records["primary_domain"] = masks["domains"].apply(first_matching_label, axis=1)
    records["included_studies_numeric"] = records["Number_of_Papers_Included"].map(parse_first_number)
    for family, mask in masks.items():
        records[f"{family}_labels"] = mask.apply(
            lambda row: "; ".join(label for label, matched in row.items() if bool(matched)), axis=1
        )

    year_counts = records["analysis_year"].dropna().astype(int).value_counts().sort_index()
    warnings: list[str] = []
    both_years = publication_year.notna() & alternate_year.notna()
    disagreements = int((publication_year[both_years] != alternate_year[both_years]).sum())
    if disagreements:
        warnings.append(
            f"{disagreements} rows have conflicting 'publication year' and 'Year' values; "
            "the pipeline uses 'publication year' and falls back to 'Year' only when missing."
        )
    return AnalysisBundle(records=records, masks=masks, counts=counts, year_counts=year_counts, warnings=warnings)


def save_figure(fig: plt.Figure, output_dir: Path, stem: str) -> None:
    fig.savefig(output_dir / f"{stem}.pdf", bbox_inches="tight", metadata={"Creator": "dtreview reproducibility pipeline"})
    fig.savefig(output_dir / f"{stem}.png", dpi=240, bbox_inches="tight")
    plt.close(fig)


def figure_prisma(counts: Mapping[str, int], warnings: Sequence[str], output_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(8.2, 10.2))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    def box(x: float, y: float, w: float, h: float, text: str, fill: str = "#F8FAFC") -> None:
        patch = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.008,rounding_size=0.01", linewidth=1.2, edgecolor="#1F2937", facecolor=fill)
        ax.add_patch(patch)
        ax.text(x + 0.018, y + h / 2, text, ha="left", va="center", fontsize=9.3, color="#111827", wrap=True)

    def arrow(x1: float, y1: float, x2: float, y2: float) -> None:
        ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=12, linewidth=1.1, color="#475569"))

    ax.text(0.5, 0.975, "PRISMA 2020 flow of records and reports", ha="center", va="top", fontsize=15, fontweight="bold", color="#0F172A")
    ax.text(0.5, 0.946, "Draft counts transcribed from the project PRISMA source", ha="center", va="top", fontsize=9, color="#475569")

    left_x, right_x, width, height = 0.08, 0.57, 0.35, 0.102
    ys = [0.80, 0.64, 0.48, 0.32, 0.15]
    left_texts = [
        f"Records identified\nDatabases: {counts['identified_databases']:,}\nRegisters: {counts['identified_registers']:,}",
        f"Records screened\n(n = {counts['screened']:,})",
        f"Reports sought for retrieval\n(n = {counts['reports_sought']:,})",
        f"Reports assessed for eligibility\n(n = {counts['reports_assessed']:,})",
        f"Studies included in review\n(n = {counts['studies_included']:,})\nReports included: {counts['reports_included']:,}",
    ]
    right_texts = [
        f"Removed before screening\nDuplicates: {counts['duplicates_removed']:,}\nAutomation: {counts['automation_removed']:,}\nOther: {counts['other_removed']:,}",
        f"Records excluded\n(n = {counts['screening_excluded']:,})",
        f"Reports not retrieved\n(n = {counts['reports_not_retrieved']:,})",
        f"Reports excluded\nReason 1: {counts['excluded_reason_1']:,}\nReason 2: {counts['excluded_reason_2']:,}\nReason 3: {counts['excluded_reason_3']:,}",
    ]
    for index, y in enumerate(ys):
        box(left_x, y, width, height, left_texts[index], fill="#EAF2F8" if index < 4 else "#E8F5E9")
        if index < 4:
            box(right_x, y, width, height, right_texts[index])
            arrow(left_x + width, y + height / 2, right_x, y + height / 2)
        if index < len(ys) - 1:
            arrow(left_x + width / 2, y, left_x + width / 2, ys[index + 1] + height)

    if warnings:
        warning = "Automated consistency check: " + " ".join(warnings)
        ax.text(0.5, 0.07, textwrap.fill(warning, 105), ha="center", va="center", fontsize=8.2, color="#9B1C1C", bbox={"boxstyle": "round,pad=0.5", "facecolor": "#FFF1F2", "edgecolor": "#FCA5A5"})
    save_figure(fig, output_dir, "fig1_prisma")


def figure_spectrum(bundle: AnalysisBundle, output_dir: Path) -> None:
    records = bundle.records.copy()
    plotted = records.dropna(subset=["hierarchy_score", "coupling_score"]).copy()
    fig, ax = plt.subplots(figsize=(9.2, 6.2))
    rng = np.random.default_rng(20260722)
    if plotted.empty:
        ax.text(0.5, 0.5, "No records had both hierarchy and coupling classifications.", ha="center", va="center")
    else:
        domain_counts = plotted["primary_domain"].value_counts()
        retained_domains = list(domain_counts.head(7).index)
        plotted["plot_domain"] = plotted["primary_domain"].where(plotted["primary_domain"].isin(retained_domains), "Other / mixed")
        domain_order = list(
            dict.fromkeys(
                retained_domains
                + (["Other / mixed"] if (plotted["plot_domain"] == "Other / mixed").any() else [])
            )
        )
        color_map = {domain: COLORS[index % len(COLORS)] for index, domain in enumerate(domain_order)}
        marker_choices = ["o", "s", "^", "D", "P", "X", "v", "h"]
        terms = list(plotted["primary_term"].value_counts().index)
        marker_map = {term: marker_choices[index % len(marker_choices)] for index, term in enumerate(terms)}
        sizes = plotted["included_studies_numeric"].fillna(5).clip(lower=1, upper=250).map(lambda n: 35 + 18 * math.sqrt(n))
        plotted["x_plot"] = plotted["coupling_score"] + rng.normal(0, 0.055, len(plotted))
        plotted["y_plot"] = plotted["hierarchy_score"] + rng.normal(0, 0.075, len(plotted))
        for (domain, term), group in plotted.groupby(["plot_domain", "primary_term"], dropna=False):
            ax.scatter(
                group["x_plot"],
                group["y_plot"],
                s=sizes.loc[group.index],
                c=color_map[domain],
                marker=marker_map[term],
                alpha=0.72,
                edgecolors="white",
                linewidths=0.7,
            )
        annotation_offsets = [(7, 7), (7, -12), (-38, 8), (-38, -13), (7, 18), (7, -22), (-44, 19), (-44, -23)]
        for offset, index in zip(annotation_offsets, plotted["included_studies_numeric"].nlargest(8).index):
            row = plotted.loc[index]
            article_id = row.get("article id")
            label = f"DP-{int(article_id)}" if pd.notna(article_id) else str(index + 1)
            ax.annotate(label, (row["x_plot"], row["y_plot"]), xytext=offset, textcoords="offset points", fontsize=7, color="#334155")

        domain_handles = [Line2D([0], [0], marker="o", color="none", markerfacecolor=color_map[d], markeredgecolor="white", markersize=7, label=d) for d in domain_order]
        term_handles = [Line2D([0], [0], marker=marker_map[t], color="#475569", linestyle="none", markersize=6, label=t) for t in terms[:6]]
        first_legend = ax.legend(handles=domain_handles, title="Healthcare domain", loc="upper left", bbox_to_anchor=(1.01, 1.0), frameon=False, fontsize=8)
        ax.add_artist(first_legend)
        ax.legend(handles=term_handles, title="Primary construct", loc="lower left", bbox_to_anchor=(1.01, 0.0), frameon=False, fontsize=8)
    ax.set_title("Digital patient spectrum: hierarchy, temporal updating, and data coupling", loc="left", fontweight="bold")
    ax.set_xlabel("Temporal/coupling score")
    ax.set_ylabel("Highest biological hierarchy level coded")
    ax.set_xticks([0, 1, 2, 3], ["Static/manual", "One-way", "Dynamic", "Bidirectional"])
    ax.set_yticks([1, 2, 3, 4, 5, 6], ["Cell/molecular", "Tissue", "Organ/system", "Person/body", "Population", "System of systems"])
    ax.grid(True, color="#E2E8F0", linewidth=0.7)
    ax.set_axisbelow(True)
    fig.text(0.01, 0.01, "Bubble area reflects the reported number of included papers (capped for readability); labels identify the eight largest reviews.", fontsize=8, color="#475569")
    fig.tight_layout(rect=(0, 0.04, 0.82, 1))
    save_figure(fig, output_dir, "fig2_spectrum")


def figure_pipeline(bundle: AnalysisBundle, output_dir: Path) -> None:
    columns = [
        ("Data inputs", bundle.counts["data_sources"].sort_values(ascending=False).head(6), "#DCEAF7"),
        ("Model families", bundle.counts["model_families"].sort_values(ascending=False).head(5), "#DDF3ED"),
        ("Clinical / research outputs", bundle.counts["functions"].sort_values(ascending=False).head(6), "#FCE8DF"),
    ]
    fig, ax = plt.subplots(figsize=(11, 6.5))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    xs = [0.035, 0.355, 0.675]
    width = 0.285
    ax.text(0.5, 0.965, "Computational pipeline reported across included reviews", ha="center", va="top", fontsize=15, fontweight="bold", color="#0F172A")
    for col_index, (heading, values, fill) in enumerate(columns):
        x = xs[col_index]
        ax.text(x + width / 2, 0.895, heading, ha="center", va="center", fontsize=11, fontweight="bold", color="#1E293B")
        top = 0.75
        box_height = 0.084
        gap = 0.019
        for row_index, (label, count) in enumerate(values.items()):
            y = top - row_index * (box_height + gap)
            patch = FancyBboxPatch((x, y), width, box_height, boxstyle="round,pad=0.008,rounding_size=0.012", linewidth=0.9, edgecolor="#94A3B8", facecolor=fill)
            ax.add_patch(patch)
            ax.text(x + 0.014, y + box_height / 2, textwrap.fill(str(label), 25), ha="left", va="center", fontsize=8.8, color="#1F2937")
            ax.text(x + width - 0.014, y + box_height / 2, str(int(count)), ha="right", va="center", fontsize=11, fontweight="bold", color="#0F172A")
        if col_index < 2:
            ax.add_patch(FancyArrowPatch((x + width + 0.012, 0.50), (xs[col_index + 1] - 0.012, 0.50), arrowstyle="-|>", mutation_scale=16, linewidth=1.8, color="#64748B"))
    ax.text(0.5, 0.045, "Counts are record-level mentions and are not mutually exclusive.", ha="center", fontsize=8.5, color="#475569")
    save_figure(fig, output_dir, "fig3_pipeline")


def figure_applications(bundle: AnalysisBundle, output_dir: Path) -> None:
    domains = list(bundle.counts["domains"].sort_values(ascending=False).head(7).index)
    functions = list(bundle.counts["functions"].sort_values(ascending=False).head(7).index)
    matrix = np.zeros((len(domains), len(functions)), dtype=int)
    for i, domain in enumerate(domains):
        for j, function in enumerate(functions):
            matrix[i, j] = int((bundle.masks["domains"][domain] & bundle.masks["functions"][function]).sum())
    fig, ax = plt.subplots(figsize=(10.2, 6.1))
    image = ax.imshow(matrix, cmap="Blues", aspect="auto")
    ax.set_xticks(range(len(functions)), [textwrap.fill(label, 16) for label in functions], rotation=35, ha="right")
    ax.set_yticks(range(len(domains)), [textwrap.fill(label, 22) for label in domains])
    threshold = matrix.max() * 0.55 if matrix.size else 0
    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            ax.text(j, i, str(matrix[i, j]), ha="center", va="center", fontsize=9, color="white" if matrix[i, j] > threshold else "#0F172A", fontweight="bold" if matrix[i, j] > threshold else "normal")
    ax.set_title("Application landscape: co-occurrence of healthcare domains and functions", loc="left", fontweight="bold")
    ax.set_xlabel("Function category")
    ax.set_ylabel("Healthcare domain")
    colorbar = fig.colorbar(image, ax=ax, fraction=0.03, pad=0.02)
    colorbar.set_label("Number of coded review records")
    fig.text(0.01, 0.01, "A review can contribute to multiple domain and function cells.", fontsize=8, color="#475569")
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    save_figure(fig, output_dir, "fig5_applications")


def horizontal_panel(ax: plt.Axes, values: pd.Series, title: str, color: str) -> None:
    ordered = values.sort_values().tail(7)
    ax.barh(range(len(ordered)), ordered.values, color=color)
    ax.set_yticks(range(len(ordered)), [textwrap.fill(str(label), 25) for label in ordered.index])
    ax.set_title(title, loc="left", fontweight="bold")
    ax.set_xlabel("Review records")
    ax.grid(axis="x", color="#E2E8F0", linewidth=0.7)
    ax.set_axisbelow(True)
    for y, value in enumerate(ordered.values):
        ax.text(value + 0.35, y, str(int(value)), va="center", fontsize=8.5, color="#334155")


def figure_validation(bundle: AnalysisBundle, output_dir: Path) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(11.2, 5.5))
    horizontal_panel(axes[0], bundle.counts["validation_methods"], "Validation approaches", "#457B9D")
    horizontal_panel(axes[1], bundle.counts["maturity"], "Reported maturity", "#2A9D8F")
    fig.suptitle("Validation and clinical-maturity landscape", x=0.02, ha="left", fontsize=15, fontweight="bold")
    fig.text(0.01, 0.01, "Counts are non-mutually exclusive record-level mentions; missing or unclear reporting remains visible in the QA report.", fontsize=8, color="#475569")
    fig.tight_layout(rect=(0, 0.04, 1, 0.93))
    save_figure(fig, output_dir, "fig4_validation")


def figure_barriers(bundle: AnalysisBundle, output_dir: Path) -> None:
    values = bundle.counts["barriers"].sort_values()
    fig, ax = plt.subplots(figsize=(9.2, 5.5))
    bars = ax.barh(range(len(values)), values.values, color=[COLORS[index % len(COLORS)] for index in range(len(values))])
    ax.set_yticks(range(len(values)), [textwrap.fill(str(label), 30) for label in values.index])
    ax.set_xlabel("Review records mentioning the barrier")
    ax.set_title("Cross-cutting barriers and implementation priorities", loc="left", fontweight="bold")
    ax.grid(axis="x", color="#E2E8F0", linewidth=0.7)
    ax.set_axisbelow(True)
    for bar, value in zip(bars, values.values):
        ax.text(value + 0.4, bar.get_y() + bar.get_height() / 2, str(int(value)), va="center", fontsize=9, color="#334155")
    fig.text(0.01, 0.01, "Categories are defined in analysis/category_rules.json and are not mutually exclusive.", fontsize=8, color="#475569")
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    save_figure(fig, output_dir, "fig6_barriers")


def write_tables(bundle: AnalysisBundle, table_dir: Path) -> None:
    records = bundle.records
    total = len(records)
    unique_titles = int(records.loc[records["normalized_title"] != "", "normalized_title"].nunique())
    missing_year = int(records["analysis_year"].isna().sum())
    duplicate_rows = total - unique_titles
    year_text = "; ".join(f"{year}: {int(count)}" for year, count in bundle.year_counts.items())

    barrier_implications = {
        "Interoperability / standards": "Use implementation profiles and shared semantics to reduce fragmented integration.",
        "Privacy / security / governance": "Include privacy, consent, security, access control, and stewardship in the conceptual framework.",
        "Ethical / legal / regulatory": "Define accountability, fairness, transparency, liability, and regulatory pathways.",
        "Computing / scalability / resources": "Treat scalable, reproducible computational environments as translational infrastructure.",
        "Clinical adoption / workflow": "Evaluate clinician trust, workflow fit, implementation burden, and workforce needs.",
        "Data quality / availability / integration": "Prioritize longitudinal, multimodal, harmonized, representative datasets.",
        "Validation / evidence / reproducibility": "Make prospective validation, benchmarks, uncertainty, and reproducibility core requirements.",
    }

    table_specs = [
        (
            "table1_characteristics.tex",
            "Characteristics of the included review evidence base.",
            "tab:characteristics",
            ["Dimension", "Current extracted signal", "Interpretive use"],
            [
                ["Evidence base", f"{total} coded rows; {unique_titles} unique normalized titles; {duplicate_rows} duplicate-title rows.", "Defines the analytic set and makes title-level duplicate reconciliation explicit."],
                ["Publication years", f"{year_text}; missing: {missing_year}.", "Shows the temporal distribution using the canonical publication-year field with documented fallback."],
                ["Protocol scope", "English-language reviews from 2020 onward addressing patient-linked physiological, biomechanical, biological, or adjacent computational models.", "Keeps eligibility interpretation tied to the PROSPERO protocol."],
                ["Dominant domains", semicolon_counts(bundle.counts["domains"], 6), "Provides the empirical basis for the application landscape."],
                ["Quality context", "Quality-appraisal and reproducibility fields are heterogeneous and incompletely populated.", "Supports descriptive synthesis and cautious interpretation rather than pooled certainty claims."],
            ],
        ),
        (
            "table2_concepts.tex",
            "Conceptual definitions and terminology across the digital patient spectrum.",
            "tab:concepts",
            ["Conceptual dimension", "Current extracted signal", "Interpretive use"],
            [
                ["Primary terminology", semicolon_counts(records["primary_term"].value_counts()), "Shows the distribution of the first identifiable primary construct per review record."],
                ["Patient specificity", f"Patient-specific/individualized ({int(combined_text(records, ['RQ2_Model_Type']).str.contains(r'individual|patient[- ]specific|personaliz', regex=True).sum())}); generic/population/archetype ({int(combined_text(records, ['RQ2_Model_Type']).str.contains(r'generic|population|archetype|cohort', regex=True).sum())}).", "Separates individualized models from population-level or archetypal constructs; categories may overlap."],
                ["Temporal orientation", f"Dynamic/continuous ({int(combined_text(records, ['RQ2_Temporal_Orientation']).str.contains(r'dynamic|continuous|real[- ]time|time[- ]dependent|longitudinal', regex=True).sum())}); explicitly static ({int(combined_text(records, ['RQ2_Temporal_Orientation']).str.contains(r'static', regex=True).sum())}).", "Tests whether digital-twin terminology is accompanied by longitudinal updating."],
                ["Data coupling", f"Bidirectional ({int(combined_text(records, ['RQ2_Data_Coupling_Mode']).str.contains(r'two[- ]?way|bi[- ]?directional|bidirectional', regex=True).sum())}); one-way ({int(combined_text(records, ['RQ2_Data_Coupling_Mode']).str.contains(r'one[- ]?way|uni[- ]?directional|unidirectional', regex=True).sum())}).", "Distinguishes model-, shadow-, and twin-like coupling claims."],
                ["Hierarchy", "Hierarchy levels are converted to an ordinal cell-to-system-of-systems scale for Figure 2; multi-level entries use the highest coded level.", "Makes the spectrum plot reproducible while preserving the row-level source text."],
            ],
        ),
        (
            "table3_pipeline.tex",
            "Modeling and computational-pipeline characteristics.",
            "tab:pipeline",
            ["Pipeline stage", "Current extracted evidence", "Table/figure implication"],
            [
                ["Data acquisition", semicolon_counts(bundle.counts["data_sources"], 7), "Populates the input layer of the computational pipeline figure."],
                ["Model construction", semicolon_counts(bundle.counts["model_families"], 6), "Organizes model families as mechanistic, data-driven, hybrid, statistical, and simulation-based approaches."],
                ["Personalization and coupling", "Patient specificity, temporal updating, and coupling direction are classified separately.", "Prevents baseline personalization from being treated automatically as a continuously updated twin."],
                ["Interoperability standards", semicolon_counts(bundle.counts["standards"]), "Shows named implementation standards separately from broad interoperability concerns."],
                ["Computing infrastructure", semicolon_counts(bundle.counts["computing"]), "Summarizes cloud, edge, hybrid, high-performance, and distributed computing mentions."],
                ["Outputs", semicolon_counts(bundle.counts["functions"], 8), "Connects pipeline outputs to the application landscape."],
            ],
        ),
        (
            "table4_validation.tex",
            "Validation strategies and evidence strength.",
            "tab:validation",
            ["Validation dimension", "Current extracted signal", "Interpretation"],
            [
                ["Validation approaches", semicolon_counts(bundle.counts["validation_methods"]), "Reports record-level mentions of empirical, simulated, cross-validation, uncertainty, expert, and conceptual approaches."],
                ["Validation data source", "Patient/clinical, simulation/benchmark, and other sources remain free-text and are preserved in the record-level output.", "Avoids imposing an unsupported pooled effect interpretation."],
                ["Fidelity and uncertainty", f"Sensitivity/uncertainty language appears in {int(bundle.masks['validation_methods']['Sensitivity / uncertainty analysis'].sum())} records.", "Shows how often model credibility was discussed explicitly."],
                ["Reproducibility", f"Explicit affirmative, code, open-source, or reproducibility language appears in {int(combined_text(records, ['RQ3_Reproducibility_Mentioned']).str.contains(r'\byes\b|reproduc|open[- ]?source|code avail|github', regex=True).sum())} records.", "Identifies an actionable transparency gap."],
                ["Clinical readiness", semicolon_counts(bundle.counts["maturity"]), "Separates conceptual promise, prototypes, validation studies, and deployed systems."],
            ],
        ),
        (
            "table5_applications.tex",
            "Applications by healthcare domain and function.",
            "tab:applications",
            ["Application layer", "Current extracted signal", "Manuscript use"],
            [
                ["Healthcare domains", semicolon_counts(bundle.counts["domains"], 9), "Populates the rows of the application heatmap."],
                ["Primary functions", semicolon_counts(bundle.counts["functions"], 8), "Populates the columns of the application heatmap."],
                ["Domain-function co-occurrence", "Each review contributes to every matched domain-function pair.", "Preserves multimodal and multidisciplinary applications without forcing one exclusive label."],
                ["Translation interpretation", "Function counts describe claims synthesized by reviews, not necessarily clinically deployed systems.", "Separates use-case breadth from maturity and implementation evidence."],
            ],
        ),
        (
            "table6_barriers.tex",
            "Barriers and limitations reported across review records.",
            "tab:barriers",
            ["Barrier category", "Current extracted signal", "Roadmap implication"],
            [
                [label, f"{int(count)} coded rows.", implication]
                for label, count in bundle.counts["barriers"].sort_values(ascending=False).items()
                for implication in [barrier_implications[label]]
            ],
        ),
        (
            "table7_recommendations.tex",
            "Future directions and recommendations derived from the extraction framework.",
            "tab:recommendations",
            ["Recommendation", "Priority", "Evidence basis and suggested action"],
            [
                ["Standardize terminology", "High", "Report explicit criteria for digital model, digital shadow, digital twin, patient digital twin, virtual patient, and in silico patient."],
                ["Define maturity levels", "High", "Classify patient specificity, hierarchy, temporal updating, data coupling, feedback, validation, and clinical integration."],
                ["Strengthen validation", "High", "Require clear validation datasets, internal/external validation where relevant, and sensitivity or uncertainty analysis."],
                ["Build interoperable infrastructure", "High", "Report applicable FHIR, HL7, DICOM, OMOP, ontology, identifier, and governance mechanisms."],
                ["Provide reproducible supplements", "High", "Publish row-level extraction outputs, executable rules, data-quality checks, and plotting datasets."],
                ["Study workflow and governance", "Medium", "Evaluate clinician trust, liability, consent, bias, interpretability, equity, and implementation burden."],
                ["Move toward prospective evidence", "High", "Distinguish conceptual promise from retrospective validation, prospective studies, and deployed clinical workflows."],
            ],
        ),
    ]

    for filename, caption, label, headers, rows in table_specs:
        write_latex_table(table_dir / filename, caption, label, headers, rows)
        pd.DataFrame(rows, columns=headers).to_csv(table_dir / filename.replace(".tex", ".csv"), index=False)


def write_snippets(bundle: AnalysisBundle, snippet_dir: Path) -> None:
    records = bundle.records
    total = len(records)
    unique_titles = int(records.loc[records["normalized_title"] != "", "normalized_title"].nunique())
    years = semicolon_counts(bundle.year_counts.rename(index=lambda year: str(int(year))).sort_index(ascending=False))
    top_domains = semicolon_counts(bundle.counts["domains"], 5)
    top_functions = semicolon_counts(bundle.counts["functions"], 5)
    top_barriers = semicolon_counts(bundle.counts["barriers"], 4)
    summary = (
        f"The current workbook contained {total} coded review records representing {unique_titles} unique normalized titles. "
        f"Publication years were {years}. The most frequently matched domains were {top_domains}; the leading function categories were {top_functions}. "
        f"The most frequently matched barrier groups were {top_barriers}. Counts are record-level, rule-based, and non-mutually exclusive."
    )
    (snippet_dir / "results_summary.tex").write_text(latex_escape(summary) + "\n", encoding="utf-8")
    abstract = (
        f"The reproducible analytic set contains {total} coded records representing {unique_titles} unique normalized review titles. "
        "Applications span multiple clinical domains, while validation, reproducibility, interoperability, governance, and clinical maturity remain unevenly reported."
    )
    (snippet_dir / "abstract_results.tex").write_text(latex_escape(abstract) + "\n", encoding="utf-8")


def write_record_level(bundle: AnalysisBundle, output_dir: Path) -> None:
    columns = [
        "article id",
        "study id",
        "author",
        "title",
        "analysis_year",
        "primary_term",
        "primary_domain",
        "hierarchy_score",
        "coupling_score",
        "included_studies_numeric",
        "data_sources_labels",
        "model_families_labels",
        "standards_labels",
        "computing_labels",
        "domains_labels",
        "functions_labels",
        "validation_methods_labels",
        "maturity_labels",
        "barriers_labels",
    ]
    bundle.records[columns].to_csv(output_dir / "record_level_classification.csv", index=False)


def write_quality_report(
    bundle: AnalysisBundle,
    prisma_warnings: Sequence[str],
    input_path: Path,
    output: Path,
) -> None:
    records = bundle.records
    duplicate_counts = records.loc[records["normalized_title"] != "", "normalized_title"].value_counts()
    duplicates = duplicate_counts[duplicate_counts > 1]
    missing = records[REQUIRED_COLUMNS].isna().mean().sort_values(ascending=False).head(12)
    warnings = list(bundle.warnings) + list(prisma_warnings)
    try:
        display_input = input_path.resolve().relative_to(ROOT.resolve()).as_posix()
    except ValueError:
        display_input = input_path.as_posix()
    lines = [
        "# Automated data-quality report",
        "",
        f"Input: `{display_input}`",
        f"Records: **{len(records)}**",
        f"Unique normalized titles: **{records.loc[records['normalized_title'] != '', 'normalized_title'].nunique()}**",
        f"Columns: **{records.shape[1]}** after derived fields",
        "",
        "## Warnings",
        "",
    ]
    if warnings:
        lines.extend(f"- {warning}" for warning in warnings)
    else:
        lines.append("- No structural or arithmetic warnings were detected.")
    lines.extend(["", "## Duplicate normalized titles", ""])
    if duplicates.empty:
        lines.append("- None detected.")
    else:
        lines.extend(f"- {title} ({int(count)} rows)" for title, count in duplicates.items())
    lines.extend(["", "## Highest missingness among required source fields", "", "| Field | Missing |", "|---|---:|"])
    lines.extend(f"| `{field}` | {rate:.1%} |" for field, rate in missing.items())
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "All thematic counts are record-level regex classifications defined in `analysis/category_rules.json`. "
            "They are intentionally non-mutually exclusive and should be reviewed after adjudication. "
            "The pipeline preserves the source workbook and writes row-level classifications to "
            "`generated/data/record_level_classification.csv` for audit.",
            "",
        ]
    )
    output.write_text("\n".join(lines), encoding="utf-8")


def generate(input_path: Path, output_dir: Path, rules_path: Path, prisma_path: Path) -> list[str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    table_dir = output_dir / "tables"
    figure_dir = output_dir / "figures"
    data_dir = output_dir / "data"
    snippet_dir = output_dir / "snippets"
    for directory in (table_dir, figure_dir, data_dir, snippet_dir):
        directory.mkdir(parents=True, exist_ok=True)
    for stale_stem in ("fig4_applications", "fig5_validation"):
        for suffix in (".pdf", ".png"):
            stale = figure_dir / f"{stale_stem}{suffix}"
            if stale.exists():
                stale.unlink()

    df = read_workbook(input_path)
    rules = load_rules(rules_path)
    bundle = analyze(df, rules)

    prisma_df = pd.read_csv(prisma_path)
    prisma_counts = {str(row.key): int(row.value) for row in prisma_df.itertuples(index=False)}
    prisma_warnings = prisma_checks(prisma_counts)

    write_tables(bundle, table_dir)
    write_snippets(bundle, snippet_dir)
    write_record_level(bundle, data_dir)
    figure_prisma(prisma_counts, prisma_warnings, figure_dir)
    figure_spectrum(bundle, figure_dir)
    figure_pipeline(bundle, figure_dir)
    figure_applications(bundle, figure_dir)
    figure_validation(bundle, figure_dir)
    figure_barriers(bundle, figure_dir)
    write_quality_report(bundle, prisma_warnings, input_path, output_dir / "data_quality_report.md")
    return list(bundle.warnings) + list(prisma_warnings)


def verify_outputs(output_dir: Path) -> None:
    expected = [
        *(output_dir / "tables" / f"table{i}_{name}.tex" for i, name in [
            (1, "characteristics"),
            (2, "concepts"),
            (3, "pipeline"),
            (4, "validation"),
            (5, "applications"),
            (6, "barriers"),
            (7, "recommendations"),
        ]),
        *(output_dir / "figures" / f"fig{i}_{name}.pdf" for i, name in [
            (1, "prisma"),
            (2, "spectrum"),
            (3, "pipeline"),
            (4, "validation"),
            (5, "applications"),
            (6, "barriers"),
        ]),
        output_dir / "data" / "record_level_classification.csv",
        output_dir / "data_quality_report.md",
    ]
    missing = [path for path in expected if not path.exists() or path.stat().st_size == 0]
    if missing:
        raise RuntimeError("Missing or empty generated artifacts: " + ", ".join(str(path) for path in missing))


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=ROOT / "data" / "raw" / "data_extraction_sheet_template.xlsx")
    parser.add_argument("--output", type=Path, default=ROOT / "generated")
    parser.add_argument("--rules", type=Path, default=ROOT / "analysis" / "category_rules.json")
    parser.add_argument("--prisma", type=Path, default=ROOT / "data" / "config" / "prisma_counts.csv")
    parser.add_argument("--check", action="store_true", help="Verify that all expected artifacts were produced.")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero if any data-quality warning is present.")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    warnings = generate(args.input, args.output, args.rules, args.prisma)
    if args.check:
        verify_outputs(args.output)
    print(json.dumps({"input": str(args.input), "output": str(args.output), "warnings": warnings}, indent=2))
    if args.strict and warnings:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
