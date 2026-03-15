# Azure DWH Real Estate Analytics

Modernes End-to-End-Portfolio-Projekt fuer ein Data Warehouse im Bereich Bau- und Immobilien-Projektcontrolling.  
Das Repository simuliert eine professionelle Analytics-Loesung mit SQL-, Python- und BI-Komponenten auf einer Architektur, die sich an Azure Synapse Analytics und Microsoft Fabric orientiert.

Die Zielsetzung ist, ein GitHub-Projekt bereitzustellen, das typische Aufgaben einer Junior Data Warehouse Engineer Rolle sichtbar macht: Datenmodellierung, Historisierung, ELT-Verarbeitung, Reporting und saubere technische Dokumentation.

## Projektueberblick

Dieses Projekt bildet ein fiktives, aber realitaetsnahes Reporting- und Analyse-Setup fuer Bau- und Immobilienprojekte ab. Typische fachliche Anforderungen sind:

- Transparenz ueber Budget, Ist-Kosten und Abweichungen
- Nachvollziehbarkeit von Projektfortschritt und Kostenentwicklung
- Historisierung von Stammdatenveraenderungen
- Konsistente KPI-Bereitstellung fuer Management-Reporting in Power BI

Im Mittelpunkt steht ein dimensionales Data-Warehouse-Modell mit Fakten- und Dimensionstabellen, einer ELT-Pipeline sowie einem Reporting-Layer fuer Projektcontrolling.

## Business Problem

In Bau- und Immobilienprojekten liegen relevante Informationen haeufig verteilt ueber mehrere operative Quellen vor, zum Beispiel Projektlisten, Budgetdateien, Rechnungsdaten, Lieferanteninformationen und Fortschrittsmeldungen. Ohne ein zentrales Data Warehouse entstehen typische Probleme:

- Keine einheitliche Sicht auf Projektstatus und Finanzkennzahlen
- Manuelle Zusammenfuehrung von Excel- oder ERP-Daten
- Fehlende Historie bei Aenderungen von Projekt-, Objekt- oder Partnerstammdaten
- Inkonsistente KPI-Definitionen zwischen Fachbereichen
- Erschwerte Auswertung von Budgetabweichungen und Kostenentwicklungen

Dieses Projekt zeigt, wie diese Daten in eine analysierbare, historisierte und reportingfaehige Struktur ueberfuehrt werden koennen.

## Architektur

Die Loesung orientiert sich an einem modernen Analytics-Stack mit klar getrennten Schichten:

1. **Source / Raw Layer**  
   Simulierte Quelldaten aus Projektcontrolling, Budgetplanung, Rechnungen und Stammdaten.

2. **Staging Layer**  
   Technische Vereinheitlichung, Datentypbereinigung, Feldmapping und erste Qualitaetspruefungen.

3. **Core Warehouse Layer**  
   Aufbau historisierter Dimensionen und faktenorientierter Tabellen im Star Schema.

4. **Reporting / Semantic Layer**  
   Bereitstellung aggregierbarer Kennzahlen und Power-BI-kompatibler Reporting-Strukturen.

5. **Consumption Layer**  
   Dashboards fuer Management, Projektleitung und Controlling.

Zielbild der Plattform:

- OneLake / Data Lake als Rohdatenablage
- Azure Synapse oder Microsoft Fabric Warehouse-orientierte SQL-Schicht
- Python fuer Ingestion, Standardisierung und Pipeline-Unterstuetzung
- SQL fuer Modellierung, Transformation und Historisierung
- Power BI fuer Visualisierung und KPI-Reporting

## Tech Stack

- **SQL** fuer DDL, Transformationen, Star Schema und Datenqualitaetspruefungen
- **Python** fuer Datenvorbereitung, Dateiverarbeitung, Testdatengenerierung und Pipeline-Orchestrierung
- **Power BI** fuer Dashboarding und semantische Berichtslogik
- **Azure Synapse Analytics / Microsoft Fabric** als architektonisches Zielmodell
- **GitHub** fuer Versionsverwaltung, Dokumentation und Portfolio-Praesentation

## Star Schema

Das Warehouse wird als dimensionales Modell aufgebaut, damit Reporting, Filterung und KPI-Bildung performant und fachlich klar moeglich sind.

Geplante Dimensionstabellen:

- `dim_project`
- `dim_property`
- `dim_customer`
- `dim_vendor`
- `dim_calendar`
- `dim_cost_category`
- `dim_contract`
- `dim_project_manager`
- `dim_status`

Geplante Faktentabellen:

- `fact_project_cost`
- `fact_project_budget`
- `fact_project_revenue`
- `fact_project_progress`
- optional `fact_project_invoice`

Beispielhafte Business-Fragen, die mit dem Star Schema beantwortet werden sollen:

- Wie entwickeln sich Ist-Kosten gegenueber dem Budget pro Projekt und Monat?
- Welche Projekte weisen die groessten Kostenabweichungen auf?
- Wie veraendert sich der Projektstatus im Zeitverlauf?
- Welche Kostenarten treiben Budgetueberschreitungen?

## SCD Type 2

Fuer ausgewaehlte Dimensionen wird **Slowly Changing Dimension Type 2** eingesetzt, um Aenderungen an Stammdaten historisch nachvollziehbar zu machen.

Das ist besonders relevant, wenn sich Attribute wie diese im Zeitverlauf aendern:

- Projektstatus
- Projektverantwortung
- Objektzuordnung
- Vertrags- oder Partnerinformationen

Typische SCD-Type-2-Attribute im Modell:

- `surrogate_key`
- `business_key`
- `valid_from`
- `valid_to`
- `is_current`
- `hash_value`

Damit kann das Reporting nicht nur den aktuellen Stand, sondern auch den gueltigen historischen Kontext eines Datensatzes analysieren.

## ELT-Pipeline

Die Verarbeitung folgt einem ELT-Ansatz:

1. Quelldaten werden im Raw Layer abgelegt.
2. Python uebernimmt Ingestion, Standardisierung und technische Vorvalidierung.
3. SQL transformiert die Daten in Staging-, Core- und Reporting-Schichten.
4. Historisierung und Surrogate-Key-Logik werden im Warehouse umgesetzt.
5. Power BI greift auf den Reporting-Layer bzw. das Star Schema zu.

Geplante Pipeline-Funktionen:

- Einlesen von CSV-/Dateiquellen
- Standardisierung von Spaltennamen und Datentypen
- Laden in Staging-Strukturen
- Aufbau von Dimensionen und Fakten
- SCD-Type-2-Verarbeitung fuer relevante Dimensionen
- Datenqualitaetschecks fuer Schluessel, Nullwerte und Konsistenz

Die lokale Demo-Pipeline ist dateibasiert umgesetzt und erzeugt nachvollziehbare Artefakte in `data/ingested/`, `data/processed/`, `data/warehouse/` und `data/quality/`.

## Power BI Output

Der Reporting-Layer ist fuer ein Controlling-Dashboard ausgelegt, das typische Anforderungen aus dem Projekt- und Immobilienumfeld abbildet.

Geplante Reporting-Bausteine:

- **Executive Overview** mit Budget, Ist, Abweichung und Projektstatus
- **Project Detail View** mit Drilldown auf Projekt-, Objekt- und Kostenartenebene
- **Budget vs. Actual Analysis** ueber Zeit
- **Cost Category Analysis** nach Lieferant, Kostenart oder Projektphase
- **Progress Monitoring** fuer Baufortschritt und Statusentwicklung

Das Ziel ist ein klar nachvollziehbares BI-Frontend auf Basis eines sauber modellierten Warehouses.

## Lokale Ausfuehrung

Die lokale Pipeline ist bewusst so aufgebaut, dass sie ohne externe Cloud-Ressourcen nachvollzogen werden kann. Die Python-Skripte verwenden nur die Standardbibliothek und arbeiten direkt auf den Demo-CSV-Dateien.

Voraussetzungen:

- Python 3.10 oder neuer

Ausfuehrungsablauf:

1. Repository klonen
2. Im Projektordner die Pipeline starten:

```bash
python src/main.py
```

Alternativ koennen die Schritte einzeln ausgefuehrt werden:

```bash
python src/ingest_data.py
python src/transform_data.py
python src/load_dimensions.py
python src/load_facts.py
python src/run_quality_checks.py
```

Der Ablauf ist:

1. Raw-Dateien nach `data/ingested/` uebernehmen
2. Felder standardisieren und in `data/processed/` schreiben
3. Dimensionen als Warehouse-Artefakte in `data/warehouse/` erzeugen
4. Fakten mit Surrogate Keys in `data/warehouse/` laden
5. Data-Quality-Checks ausfuehren und den Report nach `data/quality/quality_report.json` schreiben

Wichtige Pipeline-Dateien:

- `src/ingest_data.py`
- `src/transform_data.py`
- `src/load_dimensions.py`
- `src/load_facts.py`
- `src/main.py`
- `src/run_quality_checks.py`

## Projektstruktur

Die Repository-Struktur ist fuer eine saubere Trennung von Fachkonzept, Daten, SQL, Python und Reporting ausgelegt:

```text
azure-dwh-real-estate-analytics/
|-- docs/
|   `-- scd2-explanation.md
|-- data/
|   |-- ingested/
|   |-- processed/
|   |-- quality/
|   |-- raw/
|   `-- warehouse/
|-- src/
|   |-- data_quality_checks.py
|   |-- ingest_data.py
|   |-- load_dimensions.py
|   |-- load_facts.py
|   |-- main.py
|   |-- run_quality_checks.py
|   |-- scd_type2.py
|   `-- transform_data.py
|-- sql/
|   |-- 00_create_schemas.sql
|   |-- 01_dimensions.sql
|   |-- 02_facts.sql
|   |-- 03_scd2_dim_project.sql
|   `-- 08_quality_checks.sql
|-- tests/
|   `-- test_data_quality_checks.py
`-- README.md
```

## Projektziel fuer das Portfolio

Dieses Repository ist als Bewerbungsprojekt fuer eine Junior Data Warehouse Engineer Position gedacht. Es soll zeigen, dass folgende Themen praktisch verstanden und strukturiert umgesetzt werden koennen:

- Dimensionales Datenmodellieren
- Fakten- und Dimensionstabellen
- SCD Type 2 Historisierung
- SQL-basierte Transformationen
- Python-gestuetzte ELT-Pipelines
- BI-orientiertes Denken vom Rohdatum bis zum Dashboard
- Dokumentation und Architektur im Microsoft-Azure-Umfeld

## Status

Das Projekt enthaelt inzwischen eine lokal ausfuehrbare Demo-ELT-Pipeline inklusive Demo-Daten, dimensionalem Modell, SCD-Type-2-Logik fuer `dim_project` und Data-Quality-Checks.  
Als naechste Schritte folgen die SQL-basierte Ladeorchestrierung gegen eine echte Zielumgebung sowie der Aufbau des Power-BI-Reportings.
