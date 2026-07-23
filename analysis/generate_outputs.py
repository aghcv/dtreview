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
from matplotlib.colors import LinearSegmentedColormap, Normalize
from matplotlib.lines import Line2D
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch, Rectangle
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

INK = "#172033"
MUTED = "#526174"
GRID = "#DCE3EA"
NAVY = "#24557A"
TEAL = "#2A9D8F"
CORAL = "#D86645"
GOLD = "#D8A735"
PLUM = "#745B85"

# Colorblind-conscious qualitative palette, adapted for print and projection.
COLORS = [
    "#0072B2",
    "#009E73",
    "#D55E00",
    "#E69F00",
    "#745B85",
    "#56B4E9",
    "#6B8E5E",
    "#B45F82",
    "#64748B",
]

SEQUENTIAL_BLUE = LinearSegmentedColormap.from_list(
    "dtreview_blue", ["#F4F8FB", "#D8E8F2", "#8DBAD3", "#3F7FA7", "#173F63"]
)


def apply_publication_style() -> None:
    """Apply one restrained, journal-ready style to every generated figure."""

    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 8,
            "text.color": INK,
            "axes.labelcolor": INK,
            "axes.titlecolor": INK,
            "axes.titlesize": 8.5,
            "axes.titleweight": "semibold",
            "axes.titlepad": 5,
            "axes.labelsize": 8,
            "axes.edgecolor": GRID,
            "axes.linewidth": 0.8,
            "axes.facecolor": "white",
            "axes.spines.top": False,
            "axes.spines.right": False,
            "xtick.color": MUTED,
            "ytick.color": MUTED,
            "xtick.labelsize": 7.5,
            "ytick.labelsize": 7.5,
            "grid.color": GRID,
            "grid.linewidth": 0.7,
            "grid.alpha": 0.8,
            "legend.frameon": False,
            "figure.facecolor": "white",
            "savefig.facecolor": "white",
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
        }
    )


apply_publication_style()


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
    fig.savefig(
        output_dir / f"{stem}.pdf",
        bbox_inches="tight",
        pad_inches=0.04,
        metadata={"Creator": "dtreview reproducibility pipeline"},
    )
    fig.savefig(output_dir / f"{stem}.png", dpi=300, bbox_inches="tight", pad_inches=0.04)
    plt.close(fig)


def clean_bar_axis(ax: plt.Axes) -> None:
    ax.grid(axis="x")
    ax.set_axisbelow(True)
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(axis="y", length=0)


def figure_prisma(counts: Mapping[str, int], warnings: Sequence[str], output_dir: Path) -> None:
    del warnings  # Arithmetic warnings remain in the generated QA report, not inside the figure.

    left_texts = [
        f"Records identified from*:\nDatabases (n = {counts['identified_databases']:,})\nRegisters (n = {counts['identified_registers']:,})",
        f"Records screened\n(n = {counts['screened']:,})",
        f"Reports sought for\nretrieval\n(n = {counts['reports_sought']:,})",
        f"Reports assessed for\neligibility\n(n = {counts['reports_assessed']:,})",
        f"Studies included in review\n(n = {counts['studies_included']:,})\nReports of included\nstudies (n = {counts['reports_included']:,})",
    ]
    right_texts = [
        (
            "Records removed before\nscreening:\n"
            f"Duplicate records removed\n(n = {counts['duplicates_removed']:,})\n"
            f"Marked ineligible by\nautomation (n = {counts['automation_removed']:,})\n"
            f"Removed for other reasons\n(n = {counts['other_removed']:,})"
        ),
        f"Records excluded**\n(n = {counts['screening_excluded']:,})",
        f"Reports not retrieved\n(n = {counts['reports_not_retrieved']:,})",
        (
            "Reports excluded:\n"
            f"Reason 1 (n = {counts['excluded_reason_1']:,})\n"
            f"Reason 2 (n = {counts['excluded_reason_2']:,})\n"
            f"Reason 3 (n = {counts['excluded_reason_3']:,})"
        ),
    ]

    def add_box(
        ax: plt.Axes,
        x: float,
        y: float,
        width: float,
        height: float,
        label: str,
        fontsize: float,
        line_spacing: float = 1.08,
    ) -> None:
        ax.add_patch(
            Rectangle(
                (x, y),
                width,
                height,
                linewidth=0.8,
                edgecolor="black",
                facecolor="white",
            )
        )
        ax.text(
            x + 0.015,
            y + height / 2,
            label,
            ha="left",
            va="center",
            fontsize=fontsize,
            linespacing=line_spacing,
            color="black",
        )

    def add_arrow(ax: plt.Axes, x1: float, y1: float, x2: float, y2: float) -> None:
        ax.add_patch(
            FancyArrowPatch(
                (x1, y1),
                (x2, y2),
                arrowstyle="-|>",
                mutation_scale=9,
                linewidth=0.8,
                color="black",
                shrinkA=0,
                shrinkB=0,
            )
        )

    def add_phase(
        ax: plt.Axes,
        x: float,
        y: float,
        width: float,
        height: float,
        label: str,
        rotation: float = 90,
        fill: str = "#9DC3E6",
        fontsize: float = 6.4,
    ) -> None:
        ax.add_patch(
            FancyBboxPatch(
                (x, y),
                width,
                height,
                boxstyle="round,pad=0.002,rounding_size=0.007",
                linewidth=0.7,
                edgecolor="#3A3A3A",
                facecolor=fill,
            )
        )
        ax.text(
            x + width / 2,
            y + height / 2,
            label,
            rotation=rotation,
            ha="center",
            va="center",
            fontsize=fontsize,
            fontweight="bold",
            color="black",
        )

    fig, ax = plt.subplots(figsize=(3.35, 5.15))
    ax.set(xlim=(0, 1), ylim=(0, 1))
    ax.axis("off")
    ax.add_patch(
        FancyBboxPatch(
            (0.12, 0.936),
            0.83,
            0.048,
            boxstyle="round,pad=0.002,rounding_size=0.009",
            linewidth=0.75,
            edgecolor="#C89400",
            facecolor="#FFC000",
        )
    )
    ax.text(
        0.535,
        0.960,
        "Identification of studies via databases and registers",
        ha="center",
        va="center",
        fontsize=6.6,
        fontweight="bold",
        color="black",
    )

    left_x, right_x, width = 0.12, 0.58, 0.37
    id_y, id_h = 0.740, 0.160
    stage_h = 0.082
    ys = [id_y, 0.620, 0.485, 0.350, 0.165]
    heights = [id_h, stage_h, stage_h, stage_h, 0.105]

    add_phase(ax, 0.025, id_y, 0.060, id_h, "Identification")
    add_phase(ax, 0.025, 0.305, 0.060, 0.397, "Screening")
    add_phase(ax, 0.025, ys[-1], 0.060, heights[-1], "Included")

    for index, y in enumerate(ys):
        left_font = 7.7 if index != 4 else 7.25
        add_box(ax, left_x, y, width, heights[index], left_texts[index], left_font, 1.04)
        if index < 4:
            if index == 0:
                right_y, right_h, right_font = id_y, id_h, 6.15
            elif index == 3:
                right_y, right_h, right_font = 0.305, 0.127, 7.0
            else:
                right_y, right_h, right_font = y, heights[index], 7.55
            add_box(ax, right_x, right_y, width, right_h, right_texts[index], right_font, 1.02)
            add_arrow(ax, left_x + width, y + heights[index] / 2, right_x, y + heights[index] / 2)
        if index < len(ys) - 1:
            add_arrow(ax, left_x + width / 2, y, left_x + width / 2, ys[index + 1] + heights[index + 1])
    fig.subplots_adjust(left=0, right=1, bottom=0, top=1)
    save_figure(fig, output_dir, "fig1_prisma_single_portrait")

    fig, ax = plt.subplots(figsize=(7.1, 3.15))
    ax.set(xlim=(0, 1), ylim=(0, 1))
    ax.axis("off")
    landscape_left_texts = [
        f"Records identified*:\nDatabases: {counts['identified_databases']:,}\nRegisters: {counts['identified_registers']:,}",
        f"Records screened\n(n = {counts['screened']:,})",
        f"Reports sought\n(n = {counts['reports_sought']:,})",
        f"Reports assessed\n(n = {counts['reports_assessed']:,})",
        f"Studies included\n(n = {counts['studies_included']:,})\nReports included\n(n = {counts['reports_included']:,})",
    ]
    landscape_right_texts = [
        (
            "Removed before\nscreening:\n"
            f"Duplicates: {counts['duplicates_removed']:,}\n"
            f"Automation: {counts['automation_removed']:,}\n"
            f"Other: {counts['other_removed']:,}"
        ),
        f"Records excluded**\n(n = {counts['screening_excluded']:,})",
        f"Not retrieved\n(n = {counts['reports_not_retrieved']:,})",
        (
            "Reports excluded:\n"
            f"Reason 1: {counts['excluded_reason_1']:,}\n"
            f"Reason 2: {counts['excluded_reason_2']:,}\n"
            f"Reason 3: {counts['excluded_reason_3']:,}"
        ),
    ]

    xs = [0.012, 0.210, 0.408, 0.606, 0.804]
    width, main_y, main_h = 0.176, 0.535, 0.275
    phases = ["Identification", "Screening", "Retrieval", "Eligibility", "Included"]
    for index, x in enumerate(xs):
        add_phase(
            ax,
            x,
            0.875,
            width,
            0.075,
            phases[index],
            rotation=0,
            fill="#FFC000" if index == 0 else "#9DC3E6",
            fontsize=7.5,
        )
        main_font = 8.15 if index != 4 else 7.7
        add_box(ax, x, main_y, width, main_h, landscape_left_texts[index], main_font, 1.04)
        if index < len(xs) - 1:
            add_arrow(ax, x + width, main_y + main_h / 2, xs[index + 1], main_y + main_h / 2)
    secondary_y, secondary_h = 0.105, 0.285
    for index, x in enumerate(xs[:4]):
        secondary_font = 7.5
        add_box(ax, x, secondary_y, width, secondary_h, landscape_right_texts[index], secondary_font, 1.02)
        add_arrow(ax, x + width / 2, main_y, x + width / 2, secondary_y + secondary_h)
    fig.subplots_adjust(left=0, right=1, bottom=0, top=1)
    save_figure(fig, output_dir, "fig1_prisma_double_landscape")


def categorical_cell_offsets(count: int) -> list[tuple[float, float]]:
    """Return centered rectangular offsets that remain inside one category cell.

    The Figure 2 cells are wider than they are tall after the legend is placed,
    so the packing favors columns over rows. Five columns by three rows can
    distinguish the largest observed cell (14 records) while maintaining a
    clear margin from every categorical boundary.
    """

    if count < 0:
        raise ValueError("count must be non-negative")
    if count == 0:
        return []
    if count == 1:
        return [(0.0, 0.0)]

    columns = min(5, max(2, math.ceil(math.sqrt(count * 1.6))))
    rows = math.ceil(count / columns)
    x_spacing = 0.19
    y_extent = min(0.30, 0.15 * max(rows - 1, 1))
    y_positions = np.linspace(-y_extent, y_extent, rows) if rows > 1 else np.array([0.0])

    offsets: list[tuple[float, float]] = []
    remaining = count
    for row_index, y_offset in enumerate(y_positions):
        row_count = min(columns, remaining)
        x_positions = (np.arange(row_count) - (row_count - 1) / 2) * x_spacing
        if row_index % 2:
            x_positions = x_positions[::-1]
        offsets.extend((float(x_offset), float(y_offset)) for x_offset in x_positions)
        remaining -= row_count
    return offsets


def spread_points_within_cells(records: pd.DataFrame) -> pd.DataFrame:
    """Spread coincident categorical points without changing cell membership."""

    plotted = records.copy()
    plotted["x_plot"] = plotted["coupling_score"].astype(float)
    plotted["y_plot"] = plotted["hierarchy_score"].astype(float)

    for _, group in plotted.groupby(["coupling_score", "hierarchy_score"], sort=True):
        stable = group.assign(
            _included_sort=pd.to_numeric(group["included_studies_numeric"], errors="coerce").fillna(-1),
            _article_sort=pd.to_numeric(group["article id"], errors="coerce").fillna(np.inf),
        ).sort_values(["_included_sort", "_article_sort"], ascending=[False, True])

        offsets = categorical_cell_offsets(len(stable))
        offsets.sort(
            key=lambda pair: (pair[0] / 0.38) ** 2 + (pair[1] / 0.30) ** 2,
            reverse=True,
        )
        for record_index, (x_offset, y_offset) in zip(stable.index, offsets):
            plotted.at[record_index, "x_plot"] = float(plotted.at[record_index, "coupling_score"]) + x_offset
            plotted.at[record_index, "y_plot"] = float(plotted.at[record_index, "hierarchy_score"]) + y_offset
    return plotted


def figure_spectrum(bundle: AnalysisBundle, output_dir: Path) -> None:
    records = bundle.records.copy()
    plotted = records.dropna(subset=["hierarchy_score", "coupling_score"]).copy()
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
    sizes = plotted["included_studies_numeric"].fillna(5).clip(lower=1, upper=250).map(lambda n: 12 + 5 * math.sqrt(n))
    plotted = spread_points_within_cells(plotted)

    domain_handles = [
        Line2D([0], [0], marker="o", color="none", markerfacecolor=color_map[d], markeredgecolor="white", markersize=5.5, label=textwrap.fill(str(d), 18))
        for d in domain_order
    ]
    term_handles = [
        Line2D([0], [0], marker=marker_map[t], color="#475569", linestyle="none", markersize=5, label=textwrap.fill(str(t), 22))
        for t in terms[:6]
    ]
    size_examples = [10, 50, 200]
    size_handles = [
        Line2D([0], [0], marker="o", color="none", markerfacecolor="#A9B8C7", markeredgecolor="white", markersize=math.sqrt(12 + 5 * math.sqrt(value)), label=str(value))
        for value in size_examples
    ]

    def render(figsize: tuple[float, float], portrait: bool, stem: str) -> None:
        fig, ax = plt.subplots(figsize=figsize)
        for index, fill in enumerate(["#F5F7F9", "#F0F7F5", "#FFF8F0", "#F1F5F8"]):
            ax.axvspan(index - 0.5, index + 0.5, color=fill, zorder=0)
        for boundary in np.arange(-0.5, 4.0, 1.0):
            ax.axvline(boundary, color="#C7D3DE", linewidth=0.7, zorder=1)
        for boundary in np.arange(0.5, 7.0, 1.0):
            ax.axhline(boundary, color="#C7D3DE", linewidth=0.7, zorder=1)
        if plotted.empty:
            ax.text(0.5, 0.5, "No classified records", ha="center", va="center")
        else:
            for (domain, term), group in plotted.groupby(["plot_domain", "primary_term"], dropna=False):
                ax.scatter(
                    group["x_plot"], group["y_plot"], s=sizes.loc[group.index], c=color_map[domain],
                    marker=marker_map[term], alpha=0.80, edgecolors="white", linewidths=0.55, zorder=3,
                )
        ax.set_xlabel("Temporal orientation and coupling")
        ax.set_ylabel("Biological hierarchy")
        ax.set_xticks([0, 1, 2, 3], ["Static/\nmanual", "One-way", "Dynamic", "Bidirectional"])
        ax.set_yticks([1, 2, 3, 4, 5, 6], ["Cell/\nmolecular", "Tissue", "Organ/\nsystem", "Person/\nbody", "Population", "System of\nsystems"])
        ax.set(xlim=(-0.5, 3.5), ylim=(0.5, 6.5))
        ax.set_axisbelow(True)
        ax.spines["left"].set_color(GRID)
        ax.spines["bottom"].set_color(GRID)
        if portrait:
            first = ax.legend(handles=domain_handles, title="Domain", loc="upper left", bbox_to_anchor=(-0.28, -0.20), fontsize=5.8, title_fontsize=6.3, ncol=2, columnspacing=0.8, handletextpad=0.3, labelspacing=0.35)
            ax.add_artist(first)
            second = ax.legend(handles=term_handles, title="Construct", loc="upper left", bbox_to_anchor=(-0.28, -0.53), fontsize=5.8, title_fontsize=6.3, ncol=2, columnspacing=0.8, handletextpad=0.3, labelspacing=0.35)
            ax.add_artist(second)
            ax.legend(handles=size_handles, title="Included papers", loc="upper left", bbox_to_anchor=(-0.28, -0.82), fontsize=5.8, title_fontsize=6.3, ncol=3, columnspacing=0.8, handletextpad=0.3)
            fig.subplots_adjust(left=0.25, right=0.98, top=0.99, bottom=0.43)
        else:
            first = ax.legend(handles=domain_handles, title="Domain", loc="upper left", bbox_to_anchor=(1.01, 1.0), fontsize=6.2, title_fontsize=6.7, labelspacing=0.35)
            ax.add_artist(first)
            second = ax.legend(handles=term_handles, title="Construct", loc="center left", bbox_to_anchor=(1.01, 0.42), fontsize=6.2, title_fontsize=6.7, labelspacing=0.35)
            ax.add_artist(second)
            ax.legend(handles=size_handles, title="Included papers", loc="lower left", bbox_to_anchor=(1.01, 0.0), fontsize=6.2, title_fontsize=6.7, ncol=3, columnspacing=0.6, handletextpad=0.25)
            fig.subplots_adjust(left=0.10, right=0.79, top=0.98, bottom=0.16)
        save_figure(fig, output_dir, stem)

    render((3.35, 6.7), True, "fig2_spectrum_single_portrait")
    render((7.1, 4.25), False, "fig2_spectrum_double_landscape")


def figure_pipeline(bundle: AnalysisBundle, output_dir: Path) -> None:
    columns = [
        ("Data inputs", bundle.counts["data_sources"].sort_values(ascending=False).head(6), NAVY, "#EEF4F8"),
        ("Model families", bundle.counts["model_families"].sort_values(ascending=False).head(5), TEAL, "#EDF7F4"),
        ("Clinical / research outputs", bundle.counts["functions"].sort_values(ascending=False).head(6), CORAL, "#FFF3EE"),
    ]
    def add_item(ax: plt.Axes, x: float, y: float, width: float, height: float, label: str, count: int, accent: str, fontsize: float) -> None:
        ax.add_patch(
            FancyBboxPatch(
                (x, y), width, height, boxstyle="round,pad=0.005,rounding_size=0.008",
                linewidth=0.6, edgecolor="#CCD6DF", facecolor="white",
            )
        )
        ax.add_patch(Rectangle((x, y + 0.008), 0.006, height - 0.016, facecolor=accent, edgecolor="none"))
        ax.text(x + 0.016, y + height / 2, textwrap.fill(str(label), 24), ha="left", va="center", fontsize=fontsize, color=INK)
        ax.text(x + width - 0.016, y + height / 2, str(int(count)), ha="right", va="center", fontsize=fontsize, fontweight="semibold", color=accent)

    fig, ax = plt.subplots(figsize=(7.1, 3.7))
    ax.set(xlim=(0, 1), ylim=(0, 1))
    ax.axis("off")
    xs, width = [0.02, 0.355, 0.69], 0.29
    for col_index, (heading, values, accent, panel_fill) in enumerate(columns):
        x = xs[col_index]
        ax.add_patch(FancyBboxPatch((x - 0.01, 0.035), width + 0.02, 0.925, boxstyle="round,pad=0.006,rounding_size=0.012", linewidth=0, facecolor=panel_fill))
        ax.text(x + width / 2, 0.91, heading, ha="center", va="center", fontsize=8.3, fontweight="semibold", color=INK)
        top, box_height, gap = 0.78, 0.105, 0.025
        for row_index, (label, count) in enumerate(values.items()):
            add_item(ax, x, top - row_index * (box_height + gap), width, box_height, str(label), int(count), accent, 6.7)
        if col_index < 2:
            ax.add_patch(FancyArrowPatch((x + width + 0.010, 0.50), (xs[col_index + 1] - 0.010, 0.50), arrowstyle="-|>", mutation_scale=10, linewidth=1.0, color="#8595A5"))
    fig.subplots_adjust(left=0.01, right=0.99, bottom=0.01, top=0.99)
    save_figure(fig, output_dir, "fig3_pipeline_double_landscape")

    fig, ax = plt.subplots(figsize=(3.35, 7.0))
    ax.set(xlim=(0, 1), ylim=(0, 1))
    ax.axis("off")
    group_tops = [0.96, 0.635, 0.345]
    group_heights = [0.285, 0.25, 0.285]
    for group_index, ((heading, values, accent, panel_fill), top, group_height) in enumerate(zip(columns, group_tops, group_heights)):
        bottom = top - group_height
        ax.add_patch(FancyBboxPatch((0.05, bottom), 0.90, group_height, boxstyle="round,pad=0.005,rounding_size=0.012", linewidth=0, facecolor=panel_fill))
        ax.text(0.50, top - 0.026, heading, ha="center", va="center", fontsize=7.7, fontweight="semibold", color=INK)
        item_height = (group_height - 0.075) / len(values)
        for row_index, (label, count) in enumerate(values.items()):
            y = top - 0.060 - (row_index + 1) * item_height
            add_item(ax, 0.075, y, 0.85, item_height - 0.006, str(label), int(count), accent, 6.3)
        if group_index < 2:
            next_top = group_tops[group_index + 1]
            ax.add_patch(FancyArrowPatch((0.50, bottom - 0.010), (0.50, next_top + 0.012), arrowstyle="-|>", mutation_scale=9, linewidth=0.9, color="#8595A5"))
    fig.subplots_adjust(left=0.01, right=0.99, bottom=0.01, top=0.99)
    save_figure(fig, output_dir, "fig3_pipeline_single_portrait")


def figure_applications(bundle: AnalysisBundle, output_dir: Path) -> None:
    domains = list(bundle.counts["domains"].sort_values(ascending=False).head(7).index)
    functions = list(bundle.counts["functions"].sort_values(ascending=False).head(7).index)
    matrix = np.zeros((len(domains), len(functions)), dtype=int)
    for i, domain in enumerate(domains):
        for j, function in enumerate(functions):
            matrix[i, j] = int((bundle.masks["domains"][domain] & bundle.masks["functions"][function]).sum())
    def render(figsize: tuple[float, float], portrait: bool, stem: str) -> None:
        fig, ax = plt.subplots(figsize=figsize)
        image = ax.imshow(matrix, cmap=SEQUENTIAL_BLUE, aspect="auto", interpolation="nearest")
        wrap_x = 10 if portrait else 17
        wrap_y = 15 if portrait else 22
        rotation = 72 if portrait else 32
        tick_size = 5.2 if portrait else 6.8
        ax.set_xticks(range(len(functions)), [textwrap.fill(label, wrap_x) for label in functions], rotation=rotation, ha="right")
        ax.set_yticks(range(len(domains)), [textwrap.fill(label, wrap_y) for label in domains])
        ax.tick_params(axis="both", labelsize=tick_size)
        ax.set_xticks(np.arange(-0.5, len(functions), 1), minor=True)
        ax.set_yticks(np.arange(-0.5, len(domains), 1), minor=True)
        ax.grid(which="minor", color="white", linewidth=1.0)
        ax.tick_params(which="minor", bottom=False, left=False)
        threshold = matrix.max() * 0.55 if matrix.size else 0
        for i in range(matrix.shape[0]):
            for j in range(matrix.shape[1]):
                ax.text(j, i, str(matrix[i, j]), ha="center", va="center", fontsize=6.2 if portrait else 7.2, color="white" if matrix[i, j] > threshold else INK, fontweight="semibold" if matrix[i, j] > threshold else "normal")
        ax.set_xlabel("Function")
        ax.set_ylabel("Healthcare domain")
        colorbar = fig.colorbar(image, ax=ax, fraction=0.035, pad=0.025)
        colorbar.set_label("Review records", fontsize=6.5 if portrait else 7.5)
        colorbar.ax.tick_params(labelsize=5.8 if portrait else 6.8)
        colorbar.outline.set_visible(False)
        for spine in ax.spines.values():
            spine.set_visible(False)
        fig.tight_layout(pad=0.15)
        save_figure(fig, output_dir, stem)

    render((3.35, 5.5), True, "fig5_applications_single_portrait")
    render((7.1, 4.15), False, "fig5_applications_double_landscape")


def horizontal_panel(ax: plt.Axes, values: pd.Series, title: str, color: str, total: int, compact: bool = False) -> None:
    del total
    ordered = values.sort_values().tail(7)
    color_scale = LinearSegmentedColormap.from_list("panel", ["#DDE7EE", color])
    norm = Normalize(vmin=0, vmax=max(float(ordered.max()), 1.0))
    bars = ax.barh(
        range(len(ordered)),
        ordered.values,
        color=[color_scale(norm(value)) for value in ordered.values],
        height=0.68,
    )
    ax.set_yticks(range(len(ordered)), [textwrap.fill(str(label), 19 if compact else 25) for label in ordered.index])
    ax.tick_params(axis="y", labelsize=5.8 if compact else 6.8)
    ax.set_title(title, loc="left", fontweight="bold")
    ax.set_xlabel("Reviews")
    ax.set_xlim(0, max(float(ordered.max()) * 1.27, 1.0))
    clean_bar_axis(ax)
    for bar, value in zip(bars, ordered.values):
        ax.text(
            value + max(float(ordered.max()) * 0.018, 0.25),
            bar.get_y() + bar.get_height() / 2,
            str(int(value)),
            va="center",
            fontsize=6.2 if compact else 7.0,
            color=MUTED,
        )


def figure_validation(bundle: AnalysisBundle, output_dir: Path) -> None:
    total = len(bundle.records)
    fig, axes = plt.subplots(1, 2, figsize=(7.1, 3.55))
    horizontal_panel(axes[0], bundle.counts["validation_methods"], "Validation approaches", NAVY, total)
    horizontal_panel(axes[1], bundle.counts["maturity"], "Reported maturity", TEAL, total)
    fig.tight_layout(pad=0.2, w_pad=2.0)
    save_figure(fig, output_dir, "fig4_validation_double_landscape")

    fig, axes = plt.subplots(2, 1, figsize=(3.35, 6.3))
    horizontal_panel(axes[0], bundle.counts["validation_methods"], "Validation approaches", NAVY, total, compact=True)
    horizontal_panel(axes[1], bundle.counts["maturity"], "Reported maturity", TEAL, total, compact=True)
    fig.tight_layout(pad=0.2, h_pad=1.0)
    save_figure(fig, output_dir, "fig4_validation_single_portrait")


def figure_barriers(bundle: AnalysisBundle, output_dir: Path) -> None:
    values = bundle.counts["barriers"].sort_values()
    total = len(bundle.records)
    del total

    def render(figsize: tuple[float, float], portrait: bool, stem: str) -> None:
        fig, ax = plt.subplots(figsize=figsize)
        norm = Normalize(vmin=0, vmax=max(float(values.max()), 1.0))
        bars = ax.barh(
            range(len(values)), values.values,
            color=[SEQUENTIAL_BLUE(0.34 + 0.61 * norm(value)) for value in values.values], height=0.66,
        )
        ax.set_yticks(range(len(values)), [textwrap.fill(str(label), 19 if portrait else 32) for label in values.index])
        ax.tick_params(axis="y", labelsize=5.8 if portrait else 6.8)
        ax.set_xlabel("Reviews")
        ax.set_xlim(0, max(float(values.max()) * 1.16, 1.0))
        clean_bar_axis(ax)
        for bar, value in zip(bars, values.values):
            ax.text(value + max(float(values.max()) * 0.014, 0.25), bar.get_y() + bar.get_height() / 2, str(int(value)), va="center", fontsize=6.2 if portrait else 7.0, color=MUTED)
        fig.tight_layout(pad=0.15)
        save_figure(fig, output_dir, stem)

    render((3.35, 4.25), True, "fig6_barriers_single_portrait")
    render((7.1, 3.25), False, "fig6_barriers_double_landscape")


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
    for stale in figure_dir.glob("fig[1-6]_*.pdf"):
        stale.unlink()
    for stale in figure_dir.glob("fig[1-6]_*.png"):
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
    figure_stems = [
        "fig1_prisma",
        "fig2_spectrum",
        "fig3_pipeline",
        "fig4_validation",
        "fig5_applications",
        "fig6_barriers",
    ]
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
        *(output_dir / "figures" / f"{stem}_{variant}.{suffix}"
          for stem in figure_stems
          for variant in ("single_portrait", "double_landscape")
          for suffix in ("pdf", "png")),
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
