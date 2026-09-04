# Kahi graph-based person unicity

This plugin finds duplicate person records using validated external identifiers
and DOI relationships. Candidate pairs are name-checked and assigned an
evidence score before graph components are built. This prevents publication
coauthor networks from becoming identity components.

The graph is used only for isolation. Name compatibility is always decided by
`compare_author` from `Kahi_impactu_utils`.

## Safety model

- `dry_run` defaults to `true` and leaves the `person` collection unchanged.
- No control, version, lock, or run fields are added to person documents.
- Plans and run metadata are stored in separate collections.
- Dry-run audit records include the complete proposed target document for
  result comparison.
- Candidate identifiers are validated by source. Placeholder or malformed
  ORCID, Scopus, Scholar, Scienti, DOI, and ResearchGate values are rejected.
- Candidate edges are scored deterministically. Validated shared identifiers,
  multiple shared DOIs, exact normalized names with a DOI, and compatible names
  sharing at least two normalized tokens with a DOI are `high`.
- An alias-only match becomes `high` when the complete names share at least
  three normalized tokens and one DOI, or when the same alias has at least
  three tokens and is corroborated by four independent DOIs. Other alias-only
  and initials-only matches remain `medium` review records.
- A shared affiliation only supports a review score and never promotes it by
  itself to an automatic merge.
- Automatic clusters are built only from `high` edges, strongest first. Strong
  transitive paths are allowed, but every cluster union is blocked by a name,
  valid ORCID, or Scienti conflict between any two members.
- The canonical target is selected after identity clustering, prioritized as
  Staff, Scienti, Minciencias, Scholar, ORCID, and OpenAlex.
- `high` subclusters become disjoint automatic plans. `medium` edges are stored
  separately with `review` status and cannot suppress or enlarge a high plan.
- Different valid ORCID or Scienti identifiers block a merge.
- DOI, Scholar, Scopus, and other evidence cannot rely on one shared token when
  both records contain contradictory compound given names or surnames.
- Audit records include the compared document IDs, direct DOI status, the
  `compare_author` result, and shared or conflicting external identifiers.
- Run summaries count rejected ORCID conflicts, name mismatches, conflicting
  complete given names, and weak transitive bridges.
- A real execution requires MongoDB transaction support.
- Every touched document is compared with the discovery snapshot before the
  transaction writes anything.
- Archive, target replacement, source deletion, and audit insertion happen in
  the same transaction for each component.

## Installation

```shell
pip install ./Kahi_unicity_person_graph
```

## Usage

```yaml
config:
  database_url: mongodb://localhost:27017
  database_name: kahi
  log_database: kahi
  log_collection: log

workflow:
  unicity_person_graph:
    collection_name: person
    max_authors_threshold: 10
    single_doi_exact_name_max_authors: 50
    max_profiles_per_doi: 100
    num_jobs: 4
    component_batch_size: 100
    candidate_group_batch_size: 1000
    audit_batch_size: 100
    dry_run: true
    task:
      - scholar
      - scopus
      - researchgate
      - orcid
      - doi
    verbose: 1
```

For one shared DOI, `max_authors_threshold` is the maximum real number of work
authors that allows a compatible abbreviated name to be high confidence.
`single_doi_exact_name_max_authors` sets the corresponding limit for an exact
normalized name. A missing work count is kept for review. DOI values are
canonicalized before candidate grouping. `max_profiles_per_doi` is a separate
technical guard against pathological candidate groups; `0` disables only that
guard. The supported ID tasks are `scienti`, `linkedin`, `orcid`, `publons`,
`researchgate`, `scholar`, `scopus`, `ssrn`, and `wos`.

Different valid national IDs block a merge. Their existing source labels,
including `Cédula de Ciudadanía`, are compared exactly and are never rewritten.

`component_batch_size` limits how many disjoint graph components and snapshots
are held in memory. `audit_batch_size` limits each MongoDB dry-run insert. The
`candidate_group_batch_size` setting bounds the projected person documents used
while candidate groups are converted into validated edges. The defaults avoid
retaining raw coauthor components and complete audit payloads in RAM.

By default, auxiliary data is written to:

- `person_graph_merged`: archived absorbed documents during a real run.
- `person_graph_merged_sets`: dry-run plans and applied merge audit records.
- `person_graph_runs`: execution summaries.

The names can be overridden with `merged_collection_name`,
`sets_collection_name`, and `runs_collection_name`.

Set `dry_run: false` only after reviewing the `planned` high-confidence records
and the benchmark. Records with `review` status are never applied. MongoDB must
be a replica set or a sharded cluster so that transactions are available.
