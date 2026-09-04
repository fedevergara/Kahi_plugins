# Changelog

## 0.1.4 - 2026-09-03

### Added

- Deterministic fallback affiliations for unresolved endorsing institutions.
- Missing names for known DAM research groups.

### Changed

- Institution matching now prioritizes catalog identifiers, exact names,
  geography, and strict unambiguous similarity.
- Cataloged institutions replace obsolete synthetic relations when identified.
- Existing groups are enriched with endorsing institution relations and
  addresses on reruns.

### Fixed

- Institution aliases for ITM and other known organization name variants.
- Duplicate group addresses and unsafe shallow copies during processing.
