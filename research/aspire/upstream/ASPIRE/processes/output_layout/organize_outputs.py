#!/usr/bin/env python3
"""Normalize completed ASPIRE outputs into the public modular layout."""

from __future__ import annotations

import argparse
import csv
import hashlib
import html
import shutil
import tempfile
from collections import Counter
from pathlib import Path


PLOT_SUFFIXES = {".svg", ".pdf", ".png", ".jpg", ".jpeg", ".html"}
STRUCTURAL_PARTS = {"tables", "plots", "figures", "results", "data"}

MODULE_DIRS = {
    "batch_correction": "batch_correction",
    "outliers_corrected": "outlier_detection",
    "diversity": "diversity",
    "indicspecies": "indicator_analysis",
    "voc_correlation": "voc_correlation",
    "measurement_association": "measurement_association",
    "grouping_diagnostics": "grouping_diagnostics",
    "power_analysis": "power_analysis",
    "group_power_analysis": "power_analysis",
    "taxonomy_patient_aware": "taxonomy",
    "taxonomy_group_association": "taxonomy",
    "lung_status_analysis": "lung_status_analysis",
    "paired_group_contrast": "paired_group_contrast",
    "clustermaps": "clustermaps",
    "spieceasi": "network_analysis",
    "asv_mag_link": "asv_mag_link",
    "asv_mag_network": "asv_mag_network",
    "mito": "non_target_filtering",
    "taxonomy": "taxonomy",
    "stats": "general_stats",
}

INTERMEDIATE_DIRS = {
    "fastp", "merged", "filtered", "concat", "derep", "sina", "denoise",
    "nochimeras", "ASVs",
}

REFERENCE_DIRS = {"reference", "sina_reference", "taxonomy_reference"}
PUBLIC_OUTPUT_DIRS = ("modules", "intermediates", "references", "summary", "logs")


def replace_move(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() or destination.is_symlink():
        if destination.is_dir() and not destination.is_symlink():
            shutil.rmtree(destination)
        else:
            destination.unlink()
    shutil.move(str(source), str(destination))


def relative_payload_path(path: Path, root: Path) -> Path:
    relative = path.relative_to(root)
    cleaned = [part for part in relative.parts[:-1] if part.lower() not in STRUCTURAL_PARTS]
    return Path(*cleaned, relative.name) if cleaned else Path(relative.name)


def tree_has_files(root: Path) -> bool:
    return root.exists() and any(path.is_file() or path.is_symlink() for path in root.rglob("*"))


def move_module_tree(source_root: Path, module_root: Path) -> None:
    if not tree_has_files(source_root):
        if source_root.exists():
            shutil.rmtree(source_root, ignore_errors=True)
        return
    (module_root / "tables").mkdir(parents=True, exist_ok=True)
    (module_root / "plots").mkdir(parents=True, exist_ok=True)
    files = sorted((path for path in source_root.rglob("*") if path.is_file() or path.is_symlink()),
                   key=lambda path: len(path.parts), reverse=True)
    for source in files:
        bucket = "plots" if source.suffix.lower() in PLOT_SUFFIXES else "tables"
        destination = module_root / bucket / relative_payload_path(source, source_root)
        replace_move(source, destination)
    shutil.rmtree(source_root, ignore_errors=True)


def metadata_module(filename: str) -> str:
    name = filename.lower()
    if "sankey" in name:
        return "sankey"
    if "upset" in name or "venn" in name:
        return "upset"
    if "collector" in name:
        return "collectors_curve"
    if "bubble" in name:
        return "bubbleplotter"
    if "umap" in name or "hdbscan" in name:
        return "umap_clustering"
    return "metadata_plots"


def move_metadata_outputs(output_dir: Path, modules_dir: Path, summary_dir: Path) -> None:
    metadata_root = output_dir / "metadata"
    if not metadata_root.exists():
        return
    for source in sorted((path for path in metadata_root.rglob("*") if path.is_file() or path.is_symlink())):
        if source.name == "run_manifest.tsv":
            destination = summary_dir / "tables" / source.name
        else:
            module = metadata_module(source.name)
            (modules_dir / module / "tables").mkdir(parents=True, exist_ok=True)
            (modules_dir / module / "plots").mkdir(parents=True, exist_ok=True)
            bucket = "plots" if source.suffix.lower() in PLOT_SUFFIXES else "tables"
            destination = modules_dir / module / bucket / source.name
        replace_move(source, destination)
    shutil.rmtree(metadata_root, ignore_errors=True)


def move_selected_files(source_root: Path, module_root: Path, prefixes: tuple[str, ...]) -> None:
    if not source_root.exists():
        return
    for source in sorted(path for path in source_root.rglob("*") if path.is_file() or path.is_symlink()):
        if not source.name.startswith(prefixes):
            continue
        (module_root / "tables").mkdir(parents=True, exist_ok=True)
        (module_root / "plots").mkdir(parents=True, exist_ok=True)
        bucket = "plots" if source.suffix.lower() in PLOT_SUFFIXES else "tables"
        replace_move(source, module_root / bucket / source.name)


def move_summary_outputs(summary_dir: Path) -> None:
    if not summary_dir.exists():
        return
    files = sorted((path for path in summary_dir.rglob("*") if path.is_file() or path.is_symlink()))
    for source in files:
        relative = source.relative_to(summary_dir)
        if relative.parts and relative.parts[0] in {"tables", "plots", "report"}:
            continue
        bucket = "plots" if source.suffix.lower() in PLOT_SUFFIXES else "tables"
        replace_move(source, summary_dir / bucket / relative_payload_path(source, summary_dir))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_module_manifest(modules_dir: Path, summary_dir: Path) -> None:
    manifest = summary_dir / "tables" / "module_output_manifest.tsv"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    rows = ["module\toutput_type\trelative_path\tsize_bytes\tsha256"]
    if modules_dir.exists():
        for path in sorted(item for item in modules_dir.rglob("*") if item.is_file() and not item.is_symlink()):
            relative = path.relative_to(modules_dir)
            module = relative.parts[0]
            output_type = relative.parts[1] if len(relative.parts) > 1 else "tables"
            rows.append(f"{module}\t{output_type}\tmodules/{relative}\t{path.stat().st_size}\t{sha256(path)}")
    manifest.write_text("\n".join(rows) + "\n")

    counts: dict[tuple[str, str], list[int]] = {}
    for row in rows[1:]:
        module, output_type, _relative, size, _checksum = row.split("\t")
        values = counts.setdefault((module, output_type), [0, 0])
        values[0] += 1
        values[1] += int(size)
    summary_rows = ["module\toutput_type\tfile_count\ttotal_size_bytes"]
    for (module, output_type), (file_count, total_size) in sorted(counts.items()):
        summary_rows.append(f"{module}\t{output_type}\t{file_count}\t{total_size}")
    (summary_dir / "tables" / "module_summary.tsv").write_text("\n".join(summary_rows) + "\n")


def read_tsv(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def find_preferred(root: Path, names: tuple[str, ...], pattern: str | None = None) -> Path | None:
    if not root.is_dir():
        return None
    for name in names:
        matches = sorted(root.rglob(name))
        if matches:
            return matches[0]
    return next(iter(sorted(root.rglob(pattern))), None) if pattern else None


def sum_column(path: Path | None, column: str) -> int | None:
    if path is None:
        return None
    values = []
    for row in read_tsv(path):
        try:
            values.append(float(row[column]))
        except (KeyError, TypeError, ValueError):
            continue
    return round(sum(values)) if values else None


def data_accounting_summary(modules_dir: Path) -> str:
    output_root = modules_dir.parent
    metadata_tables = modules_dir / "metadata_plots" / "tables"
    metadata_path = find_preferred(
        metadata_tables,
        (
            "metadata_updated_micro.augmented.tsv",
            "metadata_updated_micro.tsv",
            "ASV_meta_micro.augmented.tsv",
            "ASV_meta_micro.tsv",
            "metadata_updated.tsv",
        ),
        "*metadata*.tsv",
    )
    metadata = read_tsv(metadata_path) if metadata_path else []
    columns = list(metadata[0]) if metadata else []
    columns_lower = {column.lower(): column for column in columns}

    def column_named(*names: str) -> str | None:
        return next((columns_lower[name.lower()] for name in names if name.lower() in columns_lower), None)

    sample_col = column_named("sample_id", "sampleID", "sample", "sample_code")
    patient_col = column_named("Participant_ID", "participant_id", "patient_id", "patient_code")
    metrics: list[tuple[str, int]] = []
    if metadata:
        sample_count = len({row.get(sample_col, "") for row in metadata if sample_col and row.get(sample_col, "")})
        metrics.append(("Samples analyzed", sample_count or len(metadata)))
        if patient_col:
            patient_count = len({row.get(patient_col, "") for row in metadata if row.get(patient_col, "")})
            if patient_count:
                metrics.append(("Participants", patient_count))

    augmentation_path = find_preferred(
        modules_dir / "grouping_diagnostics" / "tables",
        ("group_label_augmentation_audit.tsv",),
    )
    augmentation = read_tsv(augmentation_path) if augmentation_path else []
    if augmentation:
        source_col = next(
            (column for column in augmentation[0] if column.endswith("_assignment_source")),
            None,
        )
        if source_col:
            source_counts = Counter(row.get(source_col, "") for row in augmentation)
            metrics.append(("Observed group labels", source_counts.get("observed", 0)))
            metrics.append(("Soft-assigned group labels", source_counts.get("soft_assigned", 0)))
            metrics.append(("Unassigned group labels", source_counts.get("unassigned", 0)))

    asv_path = find_preferred(
        metadata_tables,
        ("ASV_final.micro.tsv", "ASV_final.tsv"),
        "ASV_final*.tsv",
    )
    if asv_path:
        asv_count = len(read_tsv(asv_path))
        if asv_count:
            metrics.append(("ASVs retained", asv_count))

    stats_root = modules_dir / "general_stats" / "tables"
    stat_specs = (
        ("Input read records", ("fastq_stats.tsv",)),
        ("Post-fastp read records", ("fastp_fastqs.tsv",)),
        ("Filtered merged sequences", ("filtered_fastas.tsv",)),
    )
    for label, names in stat_specs:
        value = sum_column(find_preferred(stats_root, names), "num_seqs")
        if value is not None:
            metrics.append((label, value))

    metric_html = "".join(
        f'<div class="metric"><strong>{value:,}</strong><span>{html.escape(label)}</span></div>'
        for label, value in metrics
    )

    group_columns = []
    for names in (("Type_Group", "type_group", "sample_type"), ("Case", "case", "case_control")):
        column = column_named(*names)
        if column and column not in group_columns:
            group_columns.append(column)
    if not group_columns and metadata:
        excluded = {sample_col, patient_col, column_named("Color", "colour")}
        for column in columns:
            values = {row.get(column, "") for row in metadata if row.get(column, "")}
            if column not in excluded and 1 < len(values) <= 12:
                group_columns.append(column)
            if len(group_columns) == 2:
                break

    group_html = []
    for column in group_columns:
        counts = Counter(row.get(column, "") for row in metadata if row.get(column, ""))
        rows = "".join(
            f'<tr><td>{html.escape(str(value))}</td><td>{count:,}</td></tr>'
            for value, count in sorted(counts.items(), key=lambda item: (-item[1], str(item[0])))
        )
        group_html.append(
            f'<div><h3>{html.escape(column.replace("_", " "))}</h3>'
            f'<table><thead><tr><th>Group</th><th>Samples</th></tr></thead><tbody>{rows}</tbody></table></div>'
        )

    visual_specs = (
        ("Data-loss Sankey", modules_dir / "sankey" / "plots", ("data_loss_sankey.label.html", "data_loss_sankey.label.svg", "data_loss_sankey.svg"), "*sankey*.html", "Sample and sequence retention across processing stages."),
        ("Read-depth swarmplot", modules_dir / "metadata_plots" / "plots", ("type_group_swarmplot_micro.svg", "type_group_swarmplot_micro_raw.svg"), "*swarmplot*.svg", "Final read-depth distributions across analyzed groups."),
        ("ASV overlap UpSet", modules_dir / "upset" / "plots", ("final_micro_upset.svg", "final_upset.svg"), "*upset*.svg", "ASV presence and overlap across analyzed groups."),
        ("Collector's curve", modules_dir / "collectors_curve" / "plots", ("collectors_curve_overlay.svg", "collectors_curve_faceted.svg"), "*collector*.svg", "Accumulation of observed ASVs as samples are added, providing a descriptive view of sampling coverage."),
        ("Grouping diagnostics", modules_dir / "grouping_diagnostics" / "plots", ("grouping_metric_summary.svg", "grouping_metric_summary.png"), "grouping_metric_summary.*", "PERMANOVA, silhouette, and within-versus-between distance summaries for configured metadata groupings."),
        ("Grouping ordination", modules_dir / "grouping_diagnostics" / "plots", ("grouping_ordination_bray.svg", "grouping_ordination_bray.png"), "grouping_ordination_*", "Community ordination panels colored by each configured grouping variable."),
        ("Grouping power", modules_dir / "grouping_diagnostics" / "plots", ("grouping_power.svg", "grouping_power.png"), "grouping_power.*", "Balanced resampling support curves for the configured grouping variables."),
        ("ASV-MAG network mapping", modules_dir / "asv_mag_network" / "plots" / "qc", ("asv_mag_network_mapping_classes.svg", "asv_mag_network_mapping_classes.png"), "asv_mag_network_mapping_classes.*", "Taxonomy-filtered ASV-MAG mapping classes used to annotate ASV association networks."),
    )
    visuals = []
    for title, root, names, pattern, description in visual_specs:
        path = find_preferred(root, names, pattern)
        if path:
            href = "../../" + path.relative_to(output_root).as_posix()
            if path.suffix.lower() == ".html":
                figure_class = ' class="wide"'
                media = (
                    f'<iframe src="{html.escape(href, quote=True)}" '
                    f'title="{html.escape(title, quote=True)}" loading="lazy"></iframe>'
                )
            else:
                figure_class = ""
                media = (
                    f'<img src="{html.escape(href, quote=True)}" '
                    f'alt="{html.escape(title, quote=True)}">'
                )
            visuals.append(
                f'<figure{figure_class}>{media}<figcaption><strong>{html.escape(title)}</strong>'
                f'<span>{html.escape(description)} '
                f'<a href="{html.escape(href, quote=True)}">Open separately</a></span></figcaption></figure>'
            )

    stat_links = []
    if stats_root.is_dir():
        for path in sorted(stats_root.glob("*.tsv")):
            href = "../../" + path.relative_to(output_root).as_posix()
            stat_links.append(f'<a href="{html.escape(href, quote=True)}">{html.escape(path.name)}</a>')
    sources = f'<p class="sources"><strong>General-statistics tables:</strong> {" ".join(stat_links)}</p>' if stat_links else ""

    return (
        '<section id="data-accounting"><h2>Data Accounting Summary</h2>'
        '<p>Descriptive sample, group, sequence, and feature accounting for the analyzed dataset. '
        'This section reports workflow inputs and retained data without biological interpretation.</p>'
        f'<div class="metrics">{metric_html}</div>'
        f'<div class="group-tables">{"".join(group_html)}</div>{sources}'
        f'<div class="visual-grid">{"".join(visuals)}</div></section>'
    )


def write_summary_artifacts(modules_dir: Path, summary_dir: Path) -> None:
    modules = []
    for module_dir in sorted(path for path in modules_dir.iterdir() if path.is_dir()):
        table_count = sum(1 for path in (module_dir / "tables").rglob("*") if path.is_file())
        plot_count = sum(1 for path in (module_dir / "plots").rglob("*") if path.is_file())
        modules.append((module_dir.name, table_count, plot_count))

    width = 960
    row_height = 28
    height = max(160, 90 + row_height * len(modules))
    max_count = max((tables + plots for _, tables, plots in modules), default=1)
    bars = []
    for index, (name, tables, plots) in enumerate(modules):
        y = 54 + index * row_height
        table_width = 650 * tables / max_count
        plot_width = 650 * plots / max_count
        bars.extend([
            f'<text x="180" y="{y + 14}" text-anchor="end">{html.escape(name)}</text>',
            f'<rect x="195" y="{y}" width="{table_width:.1f}" height="10" fill="#0072B2"/>',
            f'<rect x="{195 + table_width:.1f}" y="{y}" width="{plot_width:.1f}" height="10" fill="#D55E00"/>',
            f'<text x="860" y="{y + 10}" font-size="11">{tables} tables, {plots} plots</text>',
        ])
    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">'
        '<style>text{font-family:Source Sans Pro,sans-serif;font-size:12px;fill:#202020}</style>'
        '<rect width="100%" height="100%" fill="white"/>'
        '<text x="20" y="28" font-size="20" font-weight="700">ASPIRE module outputs</text>'
        '<rect x="680" y="16" width="12" height="12" fill="#0072B2"/><text x="698" y="27">Tables</text>'
        '<rect x="760" y="16" width="12" height="12" fill="#D55E00"/><text x="778" y="27">Plots</text>'
        + "".join(bars) + "</svg>"
    )
    (summary_dir / "plots" / "module_output_summary.svg").write_text(svg)

    cards = []
    for name, tables, plots in modules:
        cards.append(
            f'<article><h2>{html.escape(name.replace("_", " ").title())}</h2>'
            f'<p>{tables} table files and {plots} plot files.</p>'
            f'<a href="../../modules/{html.escape(name)}/tables/">Tables</a> '
            f'<a href="../../modules/{html.escape(name)}/plots/">Plots</a></article>'
        )
    logs_dir = summary_dir.parent / "logs"
    nextflow_outputs = [
        ("Execution report", "nextflow_report.html", "Task metrics, resource usage, status, and workflow summary."),
        ("Timeline", "nextflow_timeline.html", "Chronological task execution and concurrency."),
        ("Trace", "nextflow_trace.tsv", "Machine-readable task-level execution metrics."),
        ("Workflow DAG", "nextflow_dag.html", "Directed acyclic graph of workflow processes."),
        ("Nextflow log", "nextflow.log", "Nextflow runtime and diagnostic log."),
        ("Controller log", "controller.log", "Complete wrapper and Nextflow console stream."),
        ("Task inventory", "task_execution.tsv", "Process, hash, work directory, status, exit code, and duration."),
        ("Launch command", "launch_command.txt", "Shell-escaped ASPIRE invocation."),
        ("Nextflow version", "nextflow_version.txt", "Nextflow and runtime version information."),
    ]
    run_links = []
    for label, filename, description in nextflow_outputs:
        if (logs_dir / filename).is_file():
            run_links.append(
                f'<li><a href="../../logs/{html.escape(filename)}">{html.escape(label)}</a>'
                f' - {html.escape(description)}</li>'
            )
    if (logs_dir / "tasks").is_dir():
        run_links.append(
            '<li><a href="../../logs/tasks/">Per-task logs</a> - Commands, stdout, stderr, traces, and exit codes.</li>'
        )
    run_details = (
        '<section><h2>Nextflow run details</h2><ul>' + "".join(run_links)
        + '</ul><p>See <a href="../tables/nextflow_artifact_manifest.tsv">'
        + 'nextflow_artifact_manifest.tsv</a> for checksums of all archived run records.</p></section>'
        if run_links else ""
    )
    report = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>ASPIRE run report</title>
<style>body{{font-family:Source Sans Pro,sans-serif;max-width:1100px;margin:2rem auto;padding:0 1rem;color:#202020}}h1{{margin-bottom:.2rem}}section{{margin:2.5rem 0}}.grid,.metrics,.group-tables,.visual-grid{{display:grid;gap:1rem}}.grid{{grid-template-columns:repeat(auto-fit,minmax(240px,1fr))}}.metrics{{grid-template-columns:repeat(auto-fit,minmax(150px,1fr));margin:1.4rem 0}}.metric{{border-top:4px solid #0072B2;background:#f4f7f8;padding:1rem}}.metric strong{{display:block;font-size:1.55rem}}.metric span,figcaption span{{display:block;color:#555;margin-top:.25rem}}.group-tables{{grid-template-columns:repeat(auto-fit,minmax(260px,1fr));margin:1.4rem 0}}.group-tables h3{{margin-bottom:.4rem}}table{{border-collapse:collapse;width:100%}}th,td{{border-bottom:1px solid #ddd;padding:.45rem;text-align:left}}th:last-child,td:last-child{{text-align:right}}.visual-grid{{grid-template-columns:repeat(auto-fit,minmax(280px,1fr));margin-top:1.5rem}}figure{{margin:0;border:1px solid #d8d8d8;padding:.7rem}}figure.wide{{grid-column:1/-1}}figure iframe{{width:100%;height:520px;border:0}}figcaption{{padding:.6rem .2rem .2rem}}article{{border:1px solid #d8d8d8;border-radius:8px;padding:1rem}}article h2{{font-size:1.05rem;margin-top:0}}a{{color:#006a8e;margin-right:1rem}}li{{margin:.55rem 0}}img{{width:100%;height:auto}}.sources a{{display:inline-block;margin:.25rem .6rem .25rem 0}}</style></head>
<body><h1>ASPIRE run report</h1><p>Non-interpretive inventory of published module outputs and execution provenance.</p>
{data_accounting_summary(modules_dir)}
<section id="output-inventory"><h2>Output Inventory</h2><p>Published tables and plots organized by analytical module.</p>
<img src="../plots/module_output_summary.svg" alt="Module output counts"><div class="grid">{''.join(cards)}</div></section>
{run_details}
<p>See <a href="../tables/module_output_manifest.tsv">module_output_manifest.tsv</a> for checksums and exact paths.</p></body></html>"""
    (summary_dir / "report" / "ASPIRE_run_report.html").write_text(report)


def write_tree_manifest(root: Path, destination: Path, label: str, with_checksums: bool) -> None:
    columns = f"{label}\trelative_path\tsize_bytes" + ("\tsha256" if with_checksums else "")
    rows = [columns]
    if root.exists():
        for path in sorted(item for item in root.rglob("*") if item.is_file() and not item.is_symlink()):
            relative = path.relative_to(root)
            category = relative.parts[0] if len(relative.parts) > 1 else root.name
            row = f"{category}\t{root.name}/{relative}\t{path.stat().st_size}"
            if with_checksums:
                row += f"\t{sha256(path)}"
            rows.append(row)
    destination.write_text("\n".join(rows) + "\n")


def organize(output_dir: Path, config_path: Path | None = None) -> None:
    modules_dir = output_dir / "modules"
    intermediates_dir = output_dir / "intermediates"
    references_dir = output_dir / "references"
    summary_dir = output_dir / "summary"
    for directory in (
        modules_dir,
        intermediates_dir,
        references_dir,
        summary_dir / "tables",
        summary_dir / "plots",
        summary_dir / "report",
    ):
        directory.mkdir(parents=True, exist_ok=True)

    move_metadata_outputs(output_dir, modules_dir, summary_dir)

    move_selected_files(output_dir / "ASVs", modules_dir / "non_target_filtering", ("ASV_target",))
    move_selected_files(
        output_dir / "ASVs",
        modules_dir / "metadata_plots",
        ("ASV_final", "ASV_meta", "master_table", "metadata_updated"),
    )
    move_selected_files(
        output_dir / "spieceasi",
        modules_dir / "network_analysis",
        ("module_asv_anchor", "module_mag_anchor", "sample_module", "sample_top_modules"),
    )
    move_selected_files(
        output_dir / "spieceasi",
        modules_dir / "network_analysis",
        ("spieceasi_modules_", "spieceasi_module_", "network_modules_best_"),
    )
    move_selected_files(output_dir / "spieceasi", modules_dir / "network_analysis", ("network_",))

    # Mitochondrial variants belong with the corresponding analytical module.
    move_module_tree(output_dir / "mito" / "ASVs", modules_dir / "non_target_filtering")
    move_module_tree(output_dir / "mito" / "diversity", modules_dir / "diversity")
    move_module_tree(output_dir / "mito" / "clustermaps", modules_dir / "clustermaps")
    move_module_tree(output_dir / "mito" / "metadata", modules_dir / "metadata_plots")

    for source_name, module_name in MODULE_DIRS.items():
        move_module_tree(output_dir / source_name, modules_dir / module_name)

    for source_name in INTERMEDIATE_DIRS:
        source = output_dir / source_name
        if tree_has_files(source):
            replace_move(source, intermediates_dir / source_name)
        elif source.exists():
            shutil.rmtree(source, ignore_errors=True)

    for source_name in REFERENCE_DIRS:
        source = output_dir / source_name
        if tree_has_files(source):
            replace_move(source, references_dir / source_name)
        elif source.exists():
            shutil.rmtree(source, ignore_errors=True)

    reserved = {".aspire", "modules", "intermediates", "references", "summary", "logs"}
    for source in sorted(
        path for path in output_dir.iterdir()
        if path.is_dir() and path.name not in reserved and not path.name.startswith(".")
    ):
        move_module_tree(source, modules_dir / source.name.lower())

    for source in sorted(
        path for path in output_dir.iterdir()
        if (path.is_file() or path.is_symlink()) and not path.name.startswith(".")
    ):
        if source.suffix.lower() == ".html":
            destination = summary_dir / "report" / source.name
        elif source.suffix.lower() in PLOT_SUFFIXES:
            destination = summary_dir / "plots" / source.name
        else:
            destination = summary_dir / "tables" / source.name
        replace_move(source, destination)

    move_summary_outputs(summary_dir)
    for module_dir in (path for path in modules_dir.iterdir() if path.is_dir()):
        (module_dir / "tables").mkdir(parents=True, exist_ok=True)
        (module_dir / "plots").mkdir(parents=True, exist_ok=True)
    write_module_manifest(modules_dir, summary_dir)
    write_summary_artifacts(modules_dir, summary_dir)
    write_tree_manifest(
        references_dir,
        summary_dir / "tables" / "reference_checksums.tsv",
        "reference_group",
        with_checksums=True,
    )
    write_tree_manifest(
        intermediates_dir,
        summary_dir / "tables" / "intermediate_manifest.tsv",
        "stage",
        with_checksums=False,
    )
    write_tree_manifest(
        output_dir / "logs",
        summary_dir / "tables" / "nextflow_artifact_manifest.tsv",
        "artifact_group",
        with_checksums=True,
    )
    if config_path and config_path.is_file():
        shutil.copy2(config_path, summary_dir / "tables" / "run_config.yml")


def publish(staging_dir: Path, output_dir: Path, config_path: Path | None = None) -> None:
    if not staging_dir.is_dir():
        raise SystemExit(f"Publication staging directory not found: {staging_dir}")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.organizing-", dir=output_dir.parent))
    try:
        shutil.copytree(staging_dir, temporary, dirs_exist_ok=True, symlinks=False)
        organize(temporary, config_path)
        try:
            staging_dir.relative_to(output_dir)
            staging_inside_output = True
        except ValueError:
            staging_inside_output = False

        if staging_inside_output:
            backup = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.public-backup-", dir=output_dir.parent))
            installed = []
            moved_to_backup = []
            try:
                for name in PUBLIC_OUTPUT_DIRS:
                    source = temporary / name
                    destination = output_dir / name
                    if destination.exists() or destination.is_symlink():
                        shutil.move(str(destination), str(backup / name))
                        moved_to_backup.append(name)
                    if source.exists() or source.is_symlink():
                        shutil.move(str(source), str(destination))
                        installed.append(name)
            except Exception:
                for name in installed:
                    destination = output_dir / name
                    if destination.is_dir() and not destination.is_symlink():
                        shutil.rmtree(destination)
                    elif destination.exists() or destination.is_symlink():
                        destination.unlink()
                for name in moved_to_backup:
                    source = backup / name
                    if source.exists() or source.is_symlink():
                        shutil.move(str(source), str(output_dir / name))
                raise
            finally:
                shutil.rmtree(backup, ignore_errors=True)
                shutil.rmtree(temporary, ignore_errors=True)
            return

        backup = output_dir.with_name(f".{output_dir.name}.previous")
        if backup.exists():
            shutil.rmtree(backup)
        if output_dir.exists():
            output_dir.rename(backup)
        temporary.rename(output_dir)
        if backup.exists():
            shutil.rmtree(backup)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--staging-dir", type=Path)
    parser.add_argument("--config", type=Path)
    args = parser.parse_args()
    output_dir = args.output_dir.resolve()
    if args.staging_dir:
        publish(args.staging_dir.resolve(), output_dir, args.config.resolve() if args.config else None)
    else:
        if not output_dir.is_dir():
            raise SystemExit(f"Output directory not found: {output_dir}")
        organize(output_dir, args.config.resolve() if args.config else None)
    print(f"[output-layout] Organized published outputs under {output_dir}")


if __name__ == "__main__":
    main()
