# Learnings

- go-licenses v2.0.1 does not recognize the SDK root EUPL text. Exact declared SDK provenance must be handled explicitly; external unknown licenses remain fatal.
- gitleaks directory scans skip binary files. Stream printable binary strings through the same pinned scanner, without saving extracted secrets.
- Standard-library packages have no Go module record. Dependency notice collection must not treat their package paths as external modules.
