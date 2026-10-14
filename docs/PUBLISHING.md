# Preparing a public source release

Changing an existing repository's visibility also exposes its commit history,
older file contents, commit identities, pull requests, and Actions history.
Removing a file from the current tree does not remove its earlier versions.

Keep private development and deployment history in the private repository.
Export the reviewed source into a separate archive for a fresh public repository:

```sh
python3 deploy/export_source.py --output /outside/checkout/whatsapp-mcp-source.tar.gz
```

The export contains only the selected commit's tracked files, with no `.git`
directory, history, commit-author records, private access files, or runtime data.
It refuses to overwrite an existing file or write inside the checkout. Review
the archive before publishing; automated artifact checks do not establish the
absence of private information in arbitrary source text.

Extract into a new directory, review the contents, initialize a new Git history,
and use a GitHub no-reply author address if desired. Publishing or creating a
public repository is a separate explicit action. Keep the existing private
repository and its deployment source unchanged until any migration is planned.

Before publishing, verify the current source, exported archive, license and
upstream attribution; run CI and a secret scan; enable private vulnerability
reporting on the public repository. Do not copy private issues, Actions logs,
workflow artifacts, releases, database snapshots, or `.access` into it.
