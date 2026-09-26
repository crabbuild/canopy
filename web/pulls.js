"use strict";

const pullsView = (() => {
  const { editable, owns, date, read, pageLinks, field, select, submit } = discussion;
  const writer = repository => editable(repository) && repository.role !== "read";
  const root = repository => `/api/repositories/${encodeURIComponent(repository.name)}`;
  const endpoint = repository => `${root(repository)}/pulls`;
  const target = (current, pull, section = "discussion") => ({ repo: current.repo, view: "pull", pull, section });
  const revision = pull => pull.source.oid && pull.base.oid ? ({ pull_version: pull.version,
    source_oid: pull.source.oid, source_version: pull.source.version, base_oid: pull.base.oid, base_version: pull.base.version }) : null;
  const same = (a, b) => !!a && !!b && ["pull_version", "source_oid", "source_version", "base_oid", "base_version"].every(key => a[key] === b[key]);
  const stateLabel = pull => element("span", pull.state === "merged" ? "⑂ Merged" : pull.state === "closed" ? "✓ Closed" : pull.draft ? "◌ Draft" : "○ Open", `issue-state ${pull.state}`);
  function form(title) { const node = element("form", undefined, "discussion-editor surface"); node.append(element("h3", title)); return node; }
  function checkbox(form, label, checked) {
    const node = element("input"); node.type = "checkbox"; node.checked = checked;
    const wrapper = element("label", undefined, "check-label"); wrapper.append(node, element("span", label)); form.append(wrapper); return node;
  }
  function finished(message) { render(); notice(message); }

  async function list(repository, current, signal) {
    const params = new URLSearchParams({ after: current.after || "0" });
    if (current.state !== "all") params.set("state", current.state);
    const data = await read(repository, `${endpoint(repository)}?${params}`, signal); signal.throwIfAborted();
    const panel = element("section"), tools = element("div", undefined, "issue-tools"), filters = element("nav", undefined, "issue-filters");
    filters.setAttribute("aria-label", "Pull request state");
    for (const state of ["open", "closed", "merged", "all"]) {
      const choice = link(state[0].toUpperCase() + state.slice(1), { repo: current.repo, view: "pulls", state });
      if (current.state === state) choice.setAttribute("aria-current", "page"); filters.append(choice);
    }
    tools.append(filters);
    if (editable(repository)) tools.append(link("New pull request", { repo: current.repo, view: "new-pull" }, "button-link primary"));
    panel.append(tools);
    const rows = element("ol", undefined, "issue-list surface");
    for (const pull of data.pulls) {
      const row = element("li"), info = element("div");
      info.append(link(safe(pull.title), target(current, pull.number), "subject"), element("p", `#${pull.number} · ${pull.author} · ${date(pull.created_at_ms)}`, "discussion-meta"));
      row.append(stateLabel(pull), info); rows.append(row);
    }
    panel.append(data.pulls.length ? rows : empty("No pull requests on this page.", "Choose another state or open a pull request between two branches."), pageLinks(current, data.next_after, "Pull requests"));
    return panel;
  }

  async function newPull(repository, current, signal) {
    if (!editable(repository)) return empty("A write-scoped token is required.", "Connect with a token that permits opening pull requests.");
    const node = form("Open a pull request"), choices = [["", "Choose a branch"]];
    const base = select(node, "Base branch", choices, ""), source = select(node, "Source branch", choices, ""); base.required = source.required = true;
    let after = null, generation = null; const refs = new Map();
    const message = element("p", "", "error"); message.setAttribute("role", "alert");
    const more = button("More branches", async () => { more.disabled = true; try { await load(); } catch (error) { if (!signal.aborted) message.textContent = error.message; } finally { more.disabled = false; } });
    async function load() {
      const { refs: page } = await browse(repository, { kind: "refs", after, generation }, signal); signal.throwIfAborted();
      generation = page.generation; after = page.next_after;
      for (const ref of page.entries.filter(ref => ref.name.startsWith("refs/heads/"))) {
        if (refs.has(ref.name)) continue; refs.set(ref.name, ref.oid);
        for (const input of [base, source]) { const option = element("option", safe(ref.name.slice(11))); option.value = ref.name; input.append(option); }
      }
      more.hidden = !after;
    }
    await load(); if (refs.has(repository.default_branch)) base.value = repository.default_branch;
    const validate = () => source.setCustomValidity(source.value && source.value === base.value ? "Choose different source and base branches." : "");
    source.addEventListener("change", validate); base.addEventListener("change", validate);
    node.append(more, message);
    const title = field(node, "title", "Title", "", 256, true), body = field(node, "body", "Description", "", 16384), draft = checkbox(node, "Start as a draft", false);
    return submit({ repository, form: node, signal, label: "Open pull request",
      payload: () => ({ title: title.value, body: body.value, draft: draft.checked, source_ref: source.value, source_oid: refs.get(source.value), base_ref: base.value, base_oid: refs.get(base.value) }),
      send: payload => api(endpoint(repository), { method: "POST", body: payload, signal }),
      published: result => { navigate(target(current, result.number)); notice("Pull request opened."); },
      cancel: () => navigate({ repo: current.repo, view: "pulls" }) });
  }

  function editorial(repository, pull, path, signal) {
    const section = element("article", undefined, "discussion-post surface"), content = element("div");
    content.append(element("p", `${pull.author} · ${date(pull.created_at_ms)} · version ${pull.version}`, "discussion-meta"), element("div", pull.body || "No description provided.", "discussion-body")); section.append(content);
    if (pull.state !== "merged" && owns(repository, pull)) {
      const edit = button("Edit pull request", () => {
        edit.hidden = content.hidden = true; const node = form("Edit pull request");
        const title = field(node, "title", "Title", pull.title, 256, true), body = field(node, "body", "Description", pull.body, 16384);
        const state = select(node, "State", [["open", "Open"], ["closed", "Closed"]], pull.state), draft = checkbox(node, "Draft", pull.draft);
        submit({ repository, form: node, signal, label: "Save changes", edit: true,
          payload: () => ({ expected_version: pull.version, title: title.value, body: body.value, state: state.value, draft: draft.checked }),
          send: payload => api(path, { method: "PUT", body: payload, signal }), published: () => finished("Pull request updated."),
          cancel: () => { node.remove(); edit.hidden = content.hidden = false; } });
        section.append(node); title.focus();
      }); section.append(edit);
    }
    return section;
  }

  async function reviews(repository, current, pull, path, signal) {
    const data = await read(repository, `${path}/reviews?after=${encodeURIComponent(current.after || "0")}`, signal); signal.throwIfAborted();
    if (data.reviews.some(review => review.applicable && !same(review.revision, revision(pull)))) throw new Error("Pull revision changed. Reload before reviewing.");
    const panel = element("section", undefined, "discussion"); panel.append(element("h3", "Reviews"));
    for (const review of data.reviews) {
      const item = element("article", undefined, "discussion-post surface");
      const kind = { comment: "Commented", approve: "Approved", request_changes: "Requested changes" }[review.kind];
      item.append(element("p", `${review.reviewer} · ${kind} · ${date(review.created_at_ms)}`, "discussion-meta"));
      if (review.kind !== "comment") item.append(element("p", review.applicable ? "Applies to the current revision" : "Historical decision", "hint"));
      item.append(element("div", review.body, "discussion-body"), element("p", `Reviewed ${review.revision.source_oid.slice(0, 12)} into ${review.revision.base_oid.slice(0, 12)} · pull version ${review.revision.pull_version}`, "hint"),
        link("View reviewed changes", { ...target(current, pull.number, "changes"), review: review.number })); panel.append(item);
    }
    if (!data.reviews.length) panel.append(element("p", "No reviews on this page.", "muted"));
    panel.append(pageLinks(current, data.next_after, "Reviews"));
    const rev = revision(pull);
    if (editable(repository) && pull.state === "open" && rev && pull.source.oid !== pull.base.oid) {
      const node = form("Review this revision"), options = [["comment", "Comment"]];
      if (writer(repository) && repository.viewer.account !== pull.author && !pull.draft) options.push(["approve", "Approve"], ["request_changes", "Request changes"]);
      const kind = select(node, "Review decision", options, "comment"), body = field(node, "body", "Review text", "", 16384, true);
      kind.addEventListener("change", () => { body.required = kind.value === "comment"; body.dispatchEvent(new Event("input")); });
      submit({ repository, form: node, signal, label: "Submit review", payload: () => ({ revision: rev, kind: kind.value, body: body.value }),
        send: payload => api(`${path}/reviews`, { method: "POST", body: payload, signal }),
        published: result => { navigate({ ...current, after: String(result.number - 1) }); notice("Review submitted."); } }); panel.append(node);
    }
    return panel;
  }

  async function changes(repository, current, pull, path, signal) {
    const rev = revision(pull);
    const snapshot = current.review ? { kind: "review", number: current.review } : pull.merge ? { kind: "merged" } : rev ? { kind: "current", revision: rev } : null;
    if (!snapshot) return empty("A branch is unavailable.", "Choose a historical review from Discussion to inspect its saved changes.");
    const compare = query => api(`${path}/comparison`, { method: "POST", body: { repository_id: repository.repository_id, target: snapshot, query }, signal });
    const { comparison } = await compare({ kind: "files", after: current.after }); signal.throwIfAborted();
    const panel = element("section"), rows = element("div", undefined, "changed-files");
    const label = current.review ? `Review #${current.review}` : pull.merge ? "Merged revision" : "Current revision";
    panel.append(element("h3", label), element("p", `Source ${comparison.revision.source_oid.slice(0, 12)} into base ${comparison.revision.base_oid.slice(0, 12)} · pull version ${comparison.revision.pull_version}. Compared with merge base ${comparison.merge_base.slice(0, 12)}. Renames appear as deletion and addition.`, "hint"));
    if (current.review) panel.append(link(pull.merge ? "View merged changes" : "View current changes", target(current, pull.number, "changes")));
    for (const file of comparison.files) {
      const row = element("details", undefined, "surface"), label = file.path === null ? `Path bytes: ${file.path_base64}` : safe(file.path);
      const summary = element("summary", `${file.before ? file.after ? "Modified" : "Deleted" : "Added"} · ${label}`); row.append(summary);
      let loaded = false;
      row.addEventListener("toggle", async () => {
        if (!row.open || loaded) return; loaded = true;
        const content = element("div", undefined, "patch-content"); content.append(element("p", "Loading changes…", "file-message")); row.append(content);
        try {
          const { patch } = await compare({ kind: "patch", path_base64: file.path_base64 }); signal.throwIfAborted();
          content.replaceChildren(patchView(patch, current));
        } catch (error) { if (!signal.aborted) { content.replaceChildren(element("p", error.message, "error")); loaded = false; row.addEventListener("toggle", () => content.remove(), { once: true }); } }
      }); rows.append(row);
    }
    panel.append(rows);
    if (!comparison.files.length) panel.append(empty("No changed files.", "The selected source and merge-base trees match."));
    panel.append(pageLinks(current, comparison.next_after, "Files")); return panel;
  }

  function patchView(patch, current) {
    const panel = element("div"), metadata = element("div", undefined, "patch-metadata");
    for (const [side, label, commit] of [["before", "Merge base", patch.merge_base], ["after", "Source", patch.revision.source_oid]]) {
      const entry = patch[side], info = element("p");
      info.append(element("span", `${label}: `));
      if (entry) info.append(link(`${entry.mode} · ${entry.oid.slice(0, 12)} · Open file`, { repo: current.repo, view: "file", commit, path: patch.path_base64 }));
      else info.append(element("span", "No file"));
      metadata.append(info);
    }
    panel.append(metadata);
    const messages = { binary: "Binary or non-UTF-8 content. Read it with Git.", too_large: "File exceeds the 256 KiB diff limit. Read it with Git.", gitlink: "Submodule commit changed; content is not followed." };
    if (patch.status !== "text") { panel.append(element("p", messages[patch.status], "file-message")); return panel; }
    if (!patch.hunks.length) { panel.append(element("p", "No text changes. File presence and modes are shown above.", "file-message")); return panel; }
    const scroll = element("div", undefined, "patch-scroll"); scroll.tabIndex = 0; scroll.setAttribute("role", "region"); scroll.setAttribute("aria-label", "Unified file diff");
    const table = element("table", undefined, "patch-table"), head = element("thead"), headings = element("tr");
    for (const title of ["Old", "New", "Change", "Content"]) { const th = element("th", title); th.scope = "col"; headings.append(th); }
    head.append(headings); table.append(head);
    for (const hunk of patch.hunks) {
      const body = element("tbody"), header = element("tr", undefined, "patch-hunk");
      const range = element("td", `@@ -${hunk.old_start},${hunk.old_lines} +${hunk.new_start},${hunk.new_lines} @@`); range.colSpan = 4; header.append(range); body.append(header);
      let old = hunk.old_start, next = hunk.new_start;
      for (const line of hunk.lines) {
        const row = element("tr", undefined, `patch-${line.kind}`);
        row.append(element("td", line.kind === "add" ? "" : String(old++)), element("td", line.kind === "delete" ? "" : String(next++)),
          element("td", { add: "+", delete: "−", context: " " }[line.kind]), element("td", line.text.replaceAll("\r", "␍")));
        body.append(row);
        if (line.no_newline) {
          const marker = element("tr", undefined, "patch-marker"), cell = element("td", "\\ No newline at end of file"); cell.colSpan = 4; marker.append(cell); body.append(marker);
        }
      }
      table.append(body);
    }
    scroll.append(table); panel.append(scroll, element("p", "− Removed · + Added · ␍ Carriage return", "patch-legend")); return panel;
  }

  async function checks(repository, oid, signal) {
    const panel = element("section", undefined, "check-runs"), rows = element("ul");
    panel.append(element("h4", `Checks for ${oid.slice(0, 12)}`), rows);
    let after = null;
    const more = button("More check contexts", async () => { more.disabled = true; try { await load(); } catch (error) { if (!signal.aborted) notice(error.message); } finally { more.disabled = false; } });
    async function load() {
      const data = await read(repository, `${root(repository)}/commits/${oid}/checks${after ? `?after=${encodeURIComponent(after)}` : ""}`, signal); signal.throwIfAborted();
      for (const check of data.checks) {
        const row = element("li", `${check.context.name} · ${check.run ? check.run.state.replaceAll("_", " ") : "No run"}`);
        if (check.run?.summary) row.append(element("p", check.run.summary, "discussion-body")); rows.append(row);
      }
      after = data.next_after; more.hidden = !after;
      if (!rows.childElementCount) rows.append(element("li", "No enabled check contexts."));
    }
    await load(); panel.append(more, element("p", "Check results are a snapshot. Required checks and branch policy are rechecked when publishing.", "hint")); return panel;
  }

  async function merge(repository, current, pull, path, signal) {
    if (pull.merge) {
      const panel = empty("Pull request merged.", `Published ${pull.merge.oid} on ${date(pull.merge.merged_at_ms)}.`);
      panel.append(link("Browse merged commit", { repo: current.repo, commit: pull.merge.oid })); return panel;
    }
    const { policy } = await read(repository, `${path}/review-policy`, signal); signal.throwIfAborted();
    const rev = revision(pull), panel = element("section", undefined, "merge-panel");
    if (policy.revision && !same(rev, policy.revision)) throw new Error("Pull revision changed. Reload before reviewing or merging.");
    panel.append(element("h3", "Merge requirements"), element("p", `${policy.approvals} of ${policy.required_approvals} required approvals · ${policy.changes_requested ? "Changes requested" : "No applicable change requests"}`));
    panel.append(button("Refresh requirements", render));
    if (!policy.ready || !rev) { panel.append(empty("This pull request is not ready.", "It must be open, out of draft, and have two different live branch tips.")); return panel; }
    let candidate = null;
    if (current.candidate) {
      const data = await read(repository, `${path}/merge-candidates/${encodeURIComponent(current.candidate)}`, signal); signal.throwIfAborted(); candidate = data.candidate;
      panel.append(link("Clear candidate selection", { ...current, candidate: undefined }));
      panel.append(element("h4", `Prepared ${candidate.strategy.replaceAll("_", " ")}`), element("p", `Candidate ${candidate.id}`, "hash"));
      if (!same(candidate.revision, rev)) { panel.append(empty("Candidate belongs to an older revision.", "Prepare a new candidate for the current source and base.")); candidate = null; }
      else if (candidate.result.state === "ready") {
        panel.append(link("Inspect candidate files", { repo: current.repo, commit: candidate.result.oid }), element("pre", `git fetch origin ${data.fetch_ref}`, "candidate-ref"));
        panel.append(await checks(repository, candidate.result.oid, signal));
      } else if (candidate.result.state === "pending") {
        panel.append(element("p", "Preparation is pending. Its creator can resume the same intent.", "hint"));
        if (writer(repository) && candidate.actor === repository.viewer.account) {
          const node = form("Resume preparation"), pending = candidate;
          submit({ repository, form: node, signal, label: "Resume preparation", payload: () => ({ id: pending.id, revision: pending.revision, strategy: pending.strategy, message: pending.message }),
            send: payload => api(`${path}/merge-candidates`, { method: "POST", body: payload, signal }), published: () => finished("Candidate state refreshed.") }); panel.append(node);
        }
      } else {
        panel.append(element("p", candidate.result.state === "unrelated" ? "The branches have unrelated histories." : "Resolve these conflicts in Git, push the result, then prepare again.", "error"));
        for (const name of candidate.result.paths_base64 || []) {
          let label;
          try { label = safe(new TextDecoder("utf-8", { fatal: true }).decode(bytes(name))); }
          catch { label = `Path bytes: ${name}`; }
          panel.append(element("p", label, "conflict-path"));
        }
      }
    } else panel.append(await checks(repository, rev.source_oid, signal));
    if (!writer(repository)) { panel.append(element("p", "Repository write access and a write-scoped token are required to merge.", "hint")); return panel; }
    const prep = form("Prepare a merge candidate"), strategy = select(prep, "Merge strategy", [["merge_commit", "Merge commit"], ["squash", "Squash"]], "merge_commit");
    const message = field(prep, "body", "Commit message", `Merge pull request #${pull.number}: ${pull.title}`, 16384, true);
    submit({ repository, form: prep, signal, label: "Prepare candidate", payload: () => ({ revision: rev, strategy: strategy.value, message: message.value }),
      send: payload => api(`${path}/merge-candidates`, { method: "POST", body: payload, signal }),
      published: result => { navigate({ ...current, candidate: result.candidate.id }); notice("Candidate status updated."); } }); panel.append(prep);
    if (!policy.reviews_satisfied) { panel.append(element("p", "Required reviews are not satisfied. Preparation is available; publication waits for eligible reviews.", "hint")); return panel; }
    const selected = candidate?.result.state === "ready" ? candidate : null;
    if (current.candidate && !selected) return panel;
    const publish = form(selected ? `Publish ${selected.strategy.replaceAll("_", " ")}` : "Fast-forward merge");
    publish.append(element("p", `This advances ${safe(pull.base.reference)} using the displayed revision and marks this pull request merged.`, "hint"));
    submit({ repository, form: publish, signal, label: selected ? "Publish candidate" : "Merge fast-forward",
      payload: () => ({ revision: rev, strategy: selected?.strategy || "fast_forward", candidate_id: selected?.id || null }),
      send: payload => api(`${path}/merge`, { method: "POST", body: payload, signal }), published: () => finished("Pull request merged.") }); panel.append(publish);
    return panel;
  }

  async function detail(repository, current, signal) {
    if (!current.pull) throw new Error("Invalid pull request number.");
    const path = `${endpoint(repository)}/${current.pull}`, { pull } = await read(repository, path, signal); signal.throwIfAborted();
    const panel = element("section", undefined, "pull-detail"), title = element("div", undefined, "issue-title");
    panel.append(link("← Pull requests", { repo: current.repo, view: "pulls" }));
    title.append(element("h2", safe(pull.title)), element("span", `#${pull.number}`, "issue-number")); panel.append(title, stateLabel(pull));
    const branches = element("p", "Live branches: ", "pull-branches");
    for (const [index, branch] of [pull.source, pull.base].entries()) {
      if (index) branches.append(element("span", " → "));
      const name = safe(branch.reference.replace(/^refs\/heads\//, ""));
      branches.append(branch.oid ? link(`${name} @ ${branch.oid.slice(0, 10)}`, { repo: current.repo, commit: branch.oid, reference: branch.reference }) : element("span", `${name} (deleted)`));
    }
    panel.append(branches); const tabs = element("nav", undefined, "tabs"); tabs.setAttribute("aria-label", "Pull request views");
    for (const [label, section] of [["Discussion", "discussion"], ["Changed files", "changes"], ["Merge", "merge"]]) {
      const tab = link(label, { ...target(current, pull.number, section), candidate: current.candidate, review: section === "changes" ? current.review : undefined }, "tab"); if (section === current.section) tab.setAttribute("aria-current", "page"); tabs.append(tab);
    }
    panel.append(tabs);
    if (current.section === "changes") panel.append(await changes(repository, current, pull, path, signal));
    else if (current.section === "merge") panel.append(await merge(repository, current, pull, path, signal));
    else panel.append(editorial(repository, pull, path, signal), await reviews(repository, current, pull, path, signal));
    return panel;
  }
  return (repository, current, signal) => current.view === "new-pull" ? newPull(repository, current, signal) : current.view === "pull" ? detail(repository, current, signal) : list(repository, current, signal);
})();
