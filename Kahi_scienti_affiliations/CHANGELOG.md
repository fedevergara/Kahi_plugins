# Changelog

## 0.1.5 - 2026-09-03

### Added

- Consolidated ingestion of root Scienti institution catalogs before groups.
- Institution enrichment through existing affiliation fields for names,
  abbreviations, identifiers, addresses, establishment year, URLs, and update
  timestamps.
- Parent institution relations from `COD_INST_MACRO` and identifier-first group
  affiliation resolution.

### Changed

- Groups inherit addresses from their resolved parent institutions.
- The Scienti ranking code `00` is exposed as `Reconocido`.
