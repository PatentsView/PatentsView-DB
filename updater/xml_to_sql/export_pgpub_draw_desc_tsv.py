#!/usr/bin/env python3
"""One-off exporter for pgpub draw description text.

This script parses USPTO pgpub XML files and writes one TSV file per year.
It is intended for historical backfills where data must be regenerated from XML.
"""

import argparse
import json
import os
import re
from collections import defaultdict

from lxml import etree

from updater.xml_to_sql.parser import extract_document, process_publication_document


DEFAULT_XML_DIR = "/PatentDataVolume/PGPUBSData/pgpubs/xml_files"
DEFAULT_OUTPUT_DIR = "/airflow/PatentsView-DB/output"
DEFAULT_CONFIG = "/airflow/PatentsView-DB/resources/pgp_xml_map_v4_3-5.json"
DEFAULT_YEARS = (2005, 2006, 2007)


def build_arg_parser():
    parser = argparse.ArgumentParser(
        description="Parse pgpub XML files and export draw description text as yearly TSV files."
    )
    parser.add_argument(
        "--xml-dir",
        default=DEFAULT_XML_DIR,
        help="Directory containing pgpub XML files.",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where yearly TSV files will be written.",
    )
    parser.add_argument(
        "--parsing-config",
        default=DEFAULT_CONFIG,
        help="Path to pgpub parsing config JSON.",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=list(DEFAULT_YEARS),
        help="Years to export, e.g. --years 2005 2006 2007.",
    )
    parser.add_argument(
        "--filename-start",
        default=None,
        help="Optional inclusive lexical lower bound for XML filenames.",
    )
    parser.add_argument(
        "--filename-end",
        default=None,
        help="Optional inclusive lexical upper bound for XML filenames.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=0,
        help="Optional file cap for test runs. 0 means no cap.",
    )
    return parser


def get_xml_files(xml_dir, filename_start=None, filename_end=None, max_files=0):
    xml_files = sorted(f for f in os.listdir(xml_dir) if f.endswith(".xml"))
    if filename_start is not None:
        xml_files = [f for f in xml_files if f >= filename_start]
    if filename_end is not None:
        xml_files = [f for f in xml_files if f <= filename_end]
    if max_files and max_files > 0:
        xml_files = xml_files[:max_files]
    return [os.path.join(xml_dir, f) for f in xml_files]


def year_from_pgpub_id(pgpub_id):
    if pgpub_id is None:
        return None
    pgpub_id = str(pgpub_id).strip()
    if len(pgpub_id) < 4 or not pgpub_id[:4].isdigit():
        return None
    return int(pgpub_id[:4])


def build_writers(output_dir, years):
    os.makedirs(output_dir, exist_ok=True)
    files = {}
    writers = {}
    header_line = '"pgpub_id"\t"draw_desc_sequence"\t"draw_desc_text"\n'

    for year in years:
        path = os.path.join(output_dir, f"pg_draw_desc_text_{year}.tsv")
        fh = open(path, "w", encoding="utf-8", newline="")
        fh.write(header_line)
        files[year] = fh
        writers[year] = fh

    return files, writers


def normalize_draw_desc_text(text_value):
    text_value = str(text_value).replace("\r", " ").replace("\n", " ")
    text_value = " ".join(text_value.split())
    # Preserve literal quotes in text while keeping a TSV-safe quoted third column.
    return text_value.replace('"', '""')


def split_draw_desc_entries(section_text):
    """Split one combined brief-drawings blob into separate row-sized entries."""
    cleaned = " ".join(str(section_text).replace("\r", " ").replace("\n", " ").split())
    if not cleaned:
        return []

    figure_token_pattern = re.compile(r"\bFIG(?:S?\.?|URES?)\s*\d", flags=re.IGNORECASE)
    first_figure_match = figure_token_pattern.search(cleaned)
    if first_figure_match is None:
        return [cleaned]

    # Keep figure clauses separate after punctuation boundaries.
    figure_start_pattern = re.compile(
        r"[.;:,]\s*(?=FIG(?:S?\.?|URES?)\s*\d)", flags=re.IGNORECASE
    )
    starts = [first_figure_match.start()]
    starts.extend(m.end() for m in figure_start_pattern.finditer(cleaned))

    ordered_starts = []
    seen = set()
    for start in starts:
        if start not in seen:
            seen.add(start)
            ordered_starts.append(start)

    parts = []
    prefix = cleaned[: ordered_starts[0]].strip(" .;:,")
    if prefix:
        parts.append(prefix)

    for idx, start in enumerate(ordered_starts):
        end = ordered_starts[idx + 1] if idx + 1 < len(ordered_starts) else len(cleaned)
        chunk = cleaned[start:end].strip(" .;:,")
        if chunk:
            parts.append(chunk)

    return parts


def extract_brief_drawings_entries(text_value):
    """Extract only the Brief Description section delimited by lead/tail tags.

    We rely on text markers emitted in this field, for example:
    description="Brief Description of Drawings" end="lead" ... end="tail"
    """
    if text_value is None:
        return []

    raw_text = str(text_value)
    section_pattern = re.compile(
        r'description\s*=\s*"([^"]*)"\s+end\s*=\s*"lead"(.*?)description\s*=\s*"\1"\s+end\s*=\s*"tail"',
        flags=re.IGNORECASE | re.DOTALL,
    )

    for match in section_pattern.finditer(raw_text):
        section_name = match.group(1) or ""
        if "brief" in section_name.lower() and "draw" in section_name.lower():
            section_text = match.group(2)
            return split_draw_desc_entries(section_text)

    return []


def parse_file(xml_file, parsing_config, target_years, writers, counters):
    parser = etree.XMLParser(
        load_dtd=False, no_network=True, recover=True, huge_tree=True
    )

    for current_xml in extract_document(xml_file):
        if not current_xml or not current_xml.strip():
            continue

        try:
            patent_doc = etree.XML(current_xml.encode("utf-8"), parser=parser)
        except Exception:
            counters["xml_parse_errors"] += 1
            continue

        if patent_doc.tag == "sequence-cwu":
            continue

        try:
            data_iter = process_publication_document(patent_doc, parsing_config)
        except Exception:
            counters["document_parse_errors"] += 1
            continue

        for table_name, rows in data_iter:
            if table_name != "draw_desc_text":
                continue

            for row in rows:
                pgpub_id = row.get("document_number")
                year = year_from_pgpub_id(pgpub_id)
                if year not in target_years:
                    continue

                text_value = row.get("draw_desc_text")
                if text_value is None or str(text_value).strip() == "":
                    continue

                brief_drawings_entries = extract_brief_drawings_entries(text_value)
                if not brief_drawings_entries:
                    continue

                try:
                    base_sequence = int(row.get("draw_desc_sequence") or 1)
                except (TypeError, ValueError):
                    base_sequence = 1

                for offset, entry_text in enumerate(brief_drawings_entries):
                    draw_desc_sequence = base_sequence + offset
                    sanitized_text = normalize_draw_desc_text(entry_text)
                    line = f'{pgpub_id}\t{draw_desc_sequence}\t"{sanitized_text}"\n'

                    writers[year].write(line)
                    counters[f"rows_{year}"] += 1


def main():
    args = build_arg_parser().parse_args()

    with open(args.parsing_config, "r", encoding="utf-8") as fh:
        parsing_config = json.load(fh)

    target_years = set(args.years)
    xml_files = get_xml_files(
        args.xml_dir,
        filename_start=args.filename_start,
        filename_end=args.filename_end,
        max_files=args.max_files,
    )

    if not xml_files:
        raise SystemExit("No XML files matched the input filters.")

    files, writers = build_writers(args.output_dir, target_years)
    counters = defaultdict(int)

    try:
        print(f"Found {len(xml_files)} XML files to process")
        print(f"Target years: {sorted(target_years)}")
        for idx, xml_file in enumerate(xml_files, start=1):
            print(f"[{idx}/{len(xml_files)}] Parsing {os.path.basename(xml_file)}")
            parse_file(xml_file, parsing_config, target_years, writers, counters)
    finally:
        for fh in files.values():
            fh.close()

    print("Done.")
    print(f"XML parse errors: {counters['xml_parse_errors']}")
    print(f"Document parse errors: {counters['document_parse_errors']}")
    for year in sorted(target_years):
        print(f"Rows written for {year}: {counters[f'rows_{year}']}")


if __name__ == "__main__":
    main()
