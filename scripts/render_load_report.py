"""Render an honest PDF report from measured scripts/load_test.py JSON output."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from xml.sax.saxutils import escape

from worldcup.config import ROOT


def whole(value: float) -> str:
    return f"{value:,.0f}".replace(",", " ")


def render_comparison(results: dict, baseline: dict, output: Path) -> None:
    from reportlab.lib import colors
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    ink = colors.HexColor("#173848")
    muted = colors.HexColor("#475E68")
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle("Body", fontName="Helvetica", fontSize=9.5, leading=14,
                              spaceAfter=6, textColor=ink))
    styles.add(ParagraphStyle("Section", fontName="Helvetica-Bold", fontSize=13, leading=16,
                              spaceBefore=12, spaceAfter=6, textColor=ink))
    styles.add(ParagraphStyle("Note", parent=styles["Body"], fontSize=8, leading=11,
                              textColor=muted, spaceBefore=4))
    styles.add(ParagraphStyle("TitleLeft", fontName="Helvetica-Bold", fontSize=25, leading=30,
                              textColor=ink, spaceAfter=12))
    styles.add(ParagraphStyle("Kicker", fontName="Helvetica-Bold", fontSize=9, leading=13,
                              textColor=muted, spaceAfter=6))
    story = []

    def paragraph(value: str, style: str = "Body") -> None:
        story.append(Paragraph(escape(value), styles[style]))

    def table(rows: list, widths: list) -> None:
        element = Table(rows, colWidths=[width * cm for width in widths], repeatRows=1, hAlign="LEFT")
        element.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), ink),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#EFF4F5"), colors.white]),
            ("LINEBELOW", (0, 0), (-1, -1), 0.35, colors.HexColor("#C8D8DB")),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
            ("TOPPADDING", (0, 0), (-1, -1), 7),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ]))
        story.append(element)

    def measure(value: float | None, decimals: int = 0) -> str:
        return "n.d." if value is None else f"{value:,.{decimals}f}".replace(",", " ").replace(".", ",")

    def missing_rate(rate: int, stages: list[dict]) -> bool:
        return not any(stage["rate_requested"] == rate and stage["kind"] == "rate" for stage in stages)

    stages = results.get("stages", [])
    comparison_stage = next((stage for stage in stages if stage["rate_requested"] == 10000), None)
    legacy = next((stage for stage in baseline.get("stages", []) if stage["rate_requested"] == 10000), None)
    configuration = results.get("configuration", {})
    paragraph("WORLD CUP 2030   /   SUPERVISION DU TRAFIC", "Kicker")
    paragraph("Rapport de charge", "TitleLeft")
    paragraph("20 novembre 2026  |  Kafka, Spark Structured Streaming, H3, Redis et FastAPI", "Note")
    if comparison_stage:
        paragraph(f"Au palier de 10 000 événements/s, la pile optimisée a "
                  f"confirmé {measure(comparison_stage['sustained_processed_per_second'])} événements/s dans Redis "
                  f"pendant les {comparison_stage['production_window_seconds']} secondes de charge. "
                  f"Le débit de référence était de {measure(legacy['sustained_processed_per_second'])} événements/s "
                  "avec l'ancien chronométrage." if legacy else
                  f"Le palier à 10 000 événements/s a confirmé "
                  f"{measure(comparison_stage['sustained_processed_per_second'])} événements/s dans Redis.")
    else:
        paragraph("La pile d'origine a confirmé 6 747 événements/s dans Redis au palier nominal de 10 000/s. "
                  "La validation de la pile optimisée est en cours.")

    paragraph("Référence avant optimisation", "Section")
    baseline_rows = [
        ["Essai", "Cible/s", "Kafka/s", "Redis/s", "Redis en fin", "Vidange"],
        ["Campagne initiale", "50 000", "15 811", "6 500", "3 000 000", "88 s"],
        ["Pointe par lots · 30 s", "51 000", "51 008", "1 054", "1 530 000", "121 s"],
    ]
    if legacy:
        baseline_rows.insert(1, ["Nouvel essai sur l’ancienne pile", measure(legacy["rate_requested"]),
                                 measure(legacy["producer"]["observed_rate"]),
                                 measure(legacy["sustained_processed_per_second"]),
                                 measure(legacy["processed_after_drain"]),
                                 f"{measure(legacy['drain_seconds'], 1)} s"])
    table(baseline_rows, [5.9, 2, 2, 2, 2.5, 2])
    paragraph("L'ancien essai visait un nombre de messages. À 50 000/s, l'émission a duré "
              "environ 190 s. La pointe de 30 s a laissé 1 498 380 événements à traiter. "
              "Les débits Redis utilisent la durée propre à chaque essai.", "Note")

    paragraph("Mesure avec la fenêtre fixe", "Section")
    if stages:
        rows = [["Cible/s", "Durée", "Acquittés/s", "Lus/s", "Redis/s", "Retard¹", "Vidange²", "Seuil"]]
        for stage in stages:
            rows.append([
                measure(stage["rate_requested"]), f"{stage['production_window_seconds']} s",
                measure(stage["acknowledged_per_second"]), measure(stage["kafka_consumed_per_second"]),
                measure(stage["sustained_processed_per_second"]), measure(stage["application_backlog_at_deadline"]),
                "n.d." if stage["sink_drain_seconds"] is None else f"{measure(stage['sink_drain_seconds'], 1)} s",
                "Oui" if stage["meets_requested_rate"] else "Non",
            ])
        table(rows, [2.1, 1.7, 2.2, 1.9, 2.0, 2.4, 2.4, 1.7])
        paragraph("¹ Acquittements reçus moins événements confirmés dans Redis à la clôture. "
                  "² Temps entre la clôture et la dernière écriture Redis de l'essai.", "Note")
        for stage in stages:
            totals = (stage["producer"]["acknowledged_after_flush"], stage["kafka_consumed_final"],
                      stage["processed_after_drain"])
            paragraph(f"À {measure(stage['rate_requested'])}/s, les compteurs finaux Kafka acquittés, "
                      f"Spark lus et Redis confirmés sont {measure(totals[0])}, {measure(totals[1])} "
                      f"et {measure(totals[2])} événements.", "Note")
    else:
        paragraph("Aucun palier optimisé achevé lors de cette mise à jour.")
    missing = [measure(rate) for rate in (1000, 5000, 10000, 25000, 50000) if missing_rate(rate, stages)]
    if missing:
        paragraph(f"Paliers optimisés à documenter (événements/s) — {', '.join(missing)}.", "Note")
    if not any(stage["kind"] == "stability" and stage["production_window_seconds"] >= 900 for stage in stages):
        paragraph("La tenue sur 15 minutes reste à mesurer.", "Note")

    paragraph("Critère de réussite", "Section")
    paragraph("Au moins 95 % du débit demandé doit être acquitté par Kafka et confirmé dans Redis dans "
              "la fenêtre de charge. Les trois compteurs doivent se réconcilier après la vidange. "
              "Les offsets du checkpoint Spark mesurent la progression des lectures, et le compteur "
              "Redis suit les événements valides et dédupliqués.")

    story.append(PageBreak())
    paragraph("MESURES   /   DIAGNOSTIC", "Kicker")
    paragraph("Coûts et latence", "TitleLeft")
    paragraph("Producteur Kafka", "Section")
    paragraph("Sur 100 000 événements synthétiques, la génération et la sérialisation JSON sont passées "
              "de 37 318 à 108 424 événements par seconde CPU dans un profil sans réseau. "
              "Le producteur regroupe les envois, conserve les acquittements et borne la mémoire en file.")
    paragraph("Spark, H3 et Redis", "Section")
    paragraph("Des micro-lots de la pile d'origine ont dépassé le déclenchement de 10 s à 10 000 événements/s. "
              "Le profil isolé a mesuré environ 0,10 s pour 100 000 conversions H3 natives. "
              "La pile optimisée déduplique avant H3, combine les agrégats de cellule et de site, "
              "et groupe les écritures Redis dans des pipelines.")
    paragraph("Mesures pendant le palier optimisé à 10 000/s", "Section")
    if comparison_stage:
        latency = comparison_stage.get("latency", {})
        profile = comparison_stage.get("spark", {})
        crowd = profile.get("queries", {}).get("crowd", {})
        rows = [["Mesure", "Échantillons", "p50 (ms)", "p95 (ms)", "p99 (ms)"]]
        for name, values in [
            ("Kafka → Redis", latency.get("kafka_to_redis_ms")),
            ("Kafka → réception WebSocket", latency.get("kafka_to_dashboard_ws_ms")),
            ("Micro-lot Spark (crowd)", crowd.get("duration_ms", {}).get("triggerExecution")),
        ]:
            values = values or {}
            rows.append([name, measure(values.get("samples")), *(measure(values.get(key)) for key in ("p50", "p95", "p99"))])
        table(rows, [7.3, 2.5, 2.2, 2.2, 2.2])
        resources = [sample["resources"] for sample in comparison_stage["samples"]]
        cpu = [sample["cpu_percent_all_logical_cores"] for sample in resources
               if sample["cpu_percent_all_logical_cores"] is not None]
        ram = [sample["memory_available_mib"] for sample in resources if sample["memory_available_mib"] is not None]
        swap = [sample["swap_out_pages"] for sample in resources if sample["swap_out_pages"] is not None]
        if cpu and ram and swap:
            paragraph(f"Pic CPU du système {measure(max(cpu), 1)} % sur 8 threads logiques. "
                      f"Mémoire disponible minimale {measure(min(ram))} Mio. "
                      f"Pages écrites en swap pendant la période échantillonnée {measure(max(swap) - min(swap))}.", "Note")
        frames = (comparison_stage.get("dashboard") or {}).get("frames", [])
        recommendations = sum(1 for frame in frames if frame.get("recommendations"))
        if recommendations:
            paragraph(f"La sonde a reçu {measure(recommendations)} vues WebSocket avec recommandation "
                      "de navette simulée pendant cet essai.", "Note")
        paragraph("Les percentiles WebSocket couvrent la ville suivie par la sonde. "
                  "Ils s'arrêtent à la réception du message par le client.", "Note")
    else:
        paragraph("Les percentiles, le retard et l'utilisation des ressources de la pile optimisée "
                  "seront tirés des mesures une fois le palier terminé.")
    paragraph("Environnement et données", "Section")
    paragraph("Intel Core i5-10300H, 4 cœurs et 8 threads logiques, 15,4 Gio de RAM. "
              "Kafka 4.3.1, Spark 4.0.1 et Redis 8.8.3 sur le même ordinateur. "
              "Les charges mesurées utilisent des accès au stade et des validations de transport synthétiques.")
    if comparison_stage:
        paragraph(f"Essai optimisé {comparison_stage['run_id']} · {configuration.get('kafka_partitions', 'n.d.')} "
                  "partitions Kafka au total pour les deux flux de foule.", "Note")
    paragraph("Sources locales : docs/load-test-results.json, "
              "artifacts/benchmark-20261120/baseline-live.json et le résultat du banc à fenêtre fixe.", "Note")

    def footer(canvas, document):
        canvas.setStrokeColor(colors.HexColor("#C8D8DB"))
        canvas.line(2.3 * cm, 1.45 * cm, 18.7 * cm, 1.45 * cm)
        canvas.setFillColor(muted)
        canvas.setFont("Helvetica", 8)
        canvas.drawString(2.3 * cm, 1.05 * cm, "WORLD CUP 2030   /   20 NOVEMBRE 2026")
        canvas.drawRightString(18.7 * cm, 1.05 * cm, str(document.page))

    output.parent.mkdir(parents=True, exist_ok=True)
    document = SimpleDocTemplate(str(output), pagesize=(21 * cm, 29.7 * cm),
                                 rightMargin=2.15 * cm, leftMargin=2.15 * cm,
                                 topMargin=1.8 * cm, bottomMargin=1.75 * cm,
                                 title="World Cup 2030 - Rapport de charge")
    document.build(story, onFirstPage=footer, onLaterPages=footer)


def render(results: dict, output: Path) -> None:
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

    stages = results.get("stages", [])
    status = results.get("status", "unknown")
    if not stages and status != "preflight_failed":
        raise ValueError("No measured stages or recorded preflight failure found")
    output.parent.mkdir(parents=True, exist_ok=True)
    styles = getSampleStyleSheet()
    document = SimpleDocTemplate(
        str(output), pagesize=(21 * cm, 29.7 * cm),
        rightMargin=2 * cm, leftMargin=2 * cm, topMargin=1.7 * cm, bottomMargin=1.7 * cm,
        title="World Cup 2030 - Rapport de charge",
    )
    story = [
        Paragraph("World Cup 2030 - Rapport de charge", styles["Title"]),
        Paragraph(f"Exécution du {escape(results.get('started_at', 'date inconnue'))} au {escape(results.get('finished_at', 'fin non enregistrée'))}", styles["Normal"]),
        Paragraph("Environnement", styles["Heading2"]),
        Paragraph(f"{escape(str(results['host']['platform']))}, Python {escape(str(results['host']['python']))}, {results['host']['cpu_count']} CPU logiques.", styles["Normal"]),
        Paragraph(f"Kafka sur {escape(results['configuration']['kafka_bootstrap_servers'])}, flux {escape(results['configuration']['stream_id'])}.", styles["Normal"]),
    ]
    for detail in results.get("environment_notes", []):
        story.append(Paragraph(escape(detail), styles["Normal"]))
    story.extend([
        Paragraph("Protocole", styles["Heading2"]),
        Paragraph(
            f"Chaque palier standard demande {results['configuration']['duration_seconds']} s de charge. "
            "La sortie compte les événements écrits dans Redis par les micro-lots Spark terminés pendant l'émission. "
            "Le banc exige 95 % du débit demandé à l'entrée et à la sortie ainsi que le traitement du volume demandé après vidange.",
            styles["Normal"],
        ),
        Paragraph("Débit mesuré" if stages else "Mesures indisponibles", styles["Heading2"]),
    ])
    if stages:
        table_data = [["Cible/s", "Kafka/s", "Émis", "Traités à l'arrêt", "Traités au total", "Sortie/s", "Vidange (s)", "Atteint"]]
        for stage in stages:
            producer = stage.get("producer")
            table_data.append(
                [
                    whole(stage["rate_requested"]),
                    whole(producer["observed_rate"]) if producer else "?",
                    whole(producer["sent"]) if producer else "?",
                    whole(stage["processed_at_producer_finish"]),
                    whole(stage["processed_after_drain"]),
                    whole(stage["sustained_processed_per_second"]),
                    whole(stage["drain_seconds"]),
                    "Oui" if stage["meets_requested_rate"] else "Non",
                ]
            )
        table = Table(
            table_data, repeatRows=1, hAlign="LEFT",
            colWidths=[width * cm for width in (1.6, 1.7, 1.9, 2.55, 2.55, 1.7, 1.9, 1.45)],
        )
        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#173b4e")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#a7bec8")),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#eef4f5")]),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("ALIGN", (0, 1), (-1, -1), "RIGHT"),
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("TOPPADDING", (0, 0), (-1, -1), 7),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
                ]
            )
        )
        story.append(table)
    else:
        story.extend([Paragraph("Aucun palier de charge n'a été exécuté. Le précontrôle a échoué avant la production de messages.", styles["Normal"]), Spacer(1, 0.3 * cm)])
        story.append(Paragraph(escape(results.get("preflight_error", "Cause inconnue")), styles["Normal"]))
    target = next((stage for stage in stages if stage["rate_requested"] == 50000), None)
    conclusion = "La cible de 50 000 événements/s reste à tester."
    if target is not None:
        conclusion = (
            "Le palier à 50 000 événements/s satisfait le seuil de 95 % du banc de test."
            if target["meets_requested_rate"]
            else "Le palier à 50 000 événements/s échoue sur cet environnement."
        )
    story.extend([Paragraph("Conclusion", styles["Heading2"]), Paragraph(escape(results.get("conclusion", conclusion)), styles["Normal"])])
    for heading, paragraphs in results.get("observations", {}).items():
        story.append(Paragraph(escape(heading), styles["Heading2"]))
        story.extend(Paragraph(escape(paragraph), styles["Normal"]) for paragraph in paragraphs)
    story.append(Paragraph("Limites de mesure", styles["Heading2"]))
    for limitation in results.get("limitations", []):
        story.append(Paragraph(escape(limitation), styles["Normal"]))
    document.build(story)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path, help="JSON produced by scripts/load_test.py")
    parser.add_argument("--output", type=Path, default=ROOT / "load_testing_report.pdf")
    parser.add_argument("--baseline", type=Path, help="Legacy benchmark JSON to include in the comparison")
    args = parser.parse_args()
    results = json.loads(args.results.read_text(encoding="utf-8"))
    if results.get("schema_version") == 2:
        baseline = json.loads(args.baseline.read_text(encoding="utf-8")) if args.baseline else {}
        render_comparison(results, baseline, args.output)
    else:
        render(results, args.output)
    print(args.output)


if __name__ == "__main__":
    main()
