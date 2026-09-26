"use strict";

const issuesView = (() => {
  const { editable, owns, date, read, pageLinks, field } = discussion;
  const endpoint = repository => `/api/repositories/${encodeURIComponent(repository.name)}/issues`;
  const issueRoute = (current, issue) => ({ repo: current.repo, view: "issue", issue, state: current.state });

  function stateLabel(state) {
    return element("span", state === "open" ? "○ Open" : "✓ Closed", `issue-state ${state}`);
  }
  function metadata(record) {
    const line = element("p", `${record.author} · ${date(record.created_at_ms)}`, "discussion-meta");
    if (record.version > 1) line.append(element("span", ` · edited ${date(record.updated_at_ms)}`));
    return line;
  }
  function editor({ repository, signal, title, record, comment, send, published, cancel }) {
    const form = element("form", undefined, "discussion-editor surface");
    form.append(element("h3", title));
    const titleInput = comment ? null : field(form, "title", "Title", record?.title || "", 256, true);
    const body = field(form, "body", comment ? "Comment" : "Description", record?.body || "", 16384, comment);
    let state;
    if (record && !comment) {
      const label = element("label", "State"); state = element("select"); state.name = "state";
      state.id = "issue-edit-state"; label.htmlFor = state.id;
      for (const value of ["open", "closed"]) { const option = element("option", value === "open" ? "Open" : "Closed"); option.value = value; state.append(option); }
      state.value = record.state; form.append(label, state);
    }
    discussion.submit({ repository, form, signal, edit: !!record,
      label: record ? "Save changes" : comment ? "Add comment" : "Create issue",
      payload: () => ({ ...(record ? { expected_version: record.version } : {}),
        ...(comment ? {} : { title: titleInput.value }), body: body.value,
        ...(state ? { state: state.value } : {}) }), send, published, cancel });
    return form;
  }

  async function list(repository, current, signal) {
    const params = new URLSearchParams({ after: current.after || "0" });
    if (current.state !== "all") params.set("state", current.state);
    const data = await read(repository, `${endpoint(repository)}?${params}`, signal); signal.throwIfAborted();
    const panel = element("section"), tools = element("div", undefined, "issue-tools"), filters = element("nav", undefined, "issue-filters");
    filters.setAttribute("aria-label", "Issue state");
    for (const state of ["open", "closed", "all"]) {
      const choice = link(state[0].toUpperCase() + state.slice(1), { repo: current.repo, view: "issues", state });
      if (current.state === state) choice.setAttribute("aria-current", "page"); filters.append(choice);
    }
    tools.append(filters);
    if (editable(repository)) tools.append(link("New issue", { repo: current.repo, view: "new-issue" }, "button-link primary"));
    panel.append(tools);
    if (!data.issues.length) panel.append(empty("No issues on this page.", current.state === "open" ? "Open an issue to start a discussion or track a change." : "Choose another state or return to the first page."));
    else {
      const rows = element("ol", undefined, "issue-list surface");
      for (const issue of data.issues) {
        const row = element("li"), detail = element("div");
        detail.append(link(safe(issue.title), issueRoute(current, issue.number), "subject"), element("p", `#${issue.number} · ${issue.author} · ${date(issue.created_at_ms)}`, "discussion-meta"));
        row.append(stateLabel(issue.state), detail); rows.append(row);
      }
      panel.append(rows);
    }
    panel.append(pageLinks(current, data.next_after, "Issues")); return panel;
  }

  function newIssue(repository, current, signal) {
    if (!editable(repository)) return empty("A write-scoped token is required.", "Connect with a token that permits discussion changes to open an issue.");
    return editor({ repository, signal, title: "Open an issue", comment: false,
      send: payload => api(endpoint(repository), { method: "POST", body: payload, signal }),
      published: result => { navigate(issueRoute(current, result.number)); notice("Issue created."); },
      cancel: () => navigate({ repo: current.repo, view: "issues" }) });
  }

  async function detail(repository, current, signal) {
    if (!current.issue) throw new Error("Invalid issue number.");
    const path = `${endpoint(repository)}/${current.issue}`;
    const [{ issue }, comments] = await Promise.all([
      read(repository, path, signal), read(repository, `${path}/comments?after=${encodeURIComponent(current.after || "0")}`, signal)
    ]); signal.throwIfAborted();
    const panel = element("section", undefined, "issue-detail");
    panel.append(link("← Issues", { repo: current.repo, view: "issues", state: current.state }));
    const heading = element("div", undefined, "issue-title");
    heading.append(element("h2", safe(issue.title)), element("span", `#${issue.number}`, "issue-number")); panel.append(heading, stateLabel(issue.state));
    const original = element("article", undefined, "discussion-post surface"), content = element("div");
    content.append(metadata(issue), element("div", issue.body || "No description provided.", "discussion-body")); original.append(content);
    if (owns(repository, issue)) {
      const edit = button("Edit issue", () => {
        edit.hidden = true; content.hidden = true;
        const form = editor({ repository, signal, title: "Edit issue", record: issue, comment: false,
          send: payload => api(path, { method: "PUT", body: payload, signal }),
          published: () => { render(); notice("Issue updated."); },
          cancel: () => { form.remove(); edit.hidden = false; content.hidden = false; } });
        original.append(form); form.querySelector("input").focus();
      }); original.append(edit);
    }
    panel.append(original);
    const discussion = element("section", undefined, "discussion"); discussion.setAttribute("aria-label", "Comments");
    discussion.append(element("h3", "Discussion"));
    for (const comment of comments.comments) {
      const post = element("article", undefined, "discussion-post surface"), content = element("div");
      content.append(metadata(comment), element("div", comment.body, "discussion-body")); post.append(content);
      if (owns(repository, comment)) {
        const edit = button("Edit comment", () => {
          edit.hidden = true; content.hidden = true;
          const form = editor({ repository, signal, title: "Edit comment", record: comment, comment: true,
            send: payload => api(`${path}/comments/${comment.number}`, { method: "PUT", body: payload, signal }),
            published: () => { render(); notice("Comment updated."); },
            cancel: () => { form.remove(); edit.hidden = false; content.hidden = false; } });
          post.append(form); form.querySelector("textarea").focus();
        }); post.append(edit);
      }
      discussion.append(post);
    }
    if (!comments.comments.length) discussion.append(element("p", "No comments on this page.", "muted"));
    discussion.append(pageLinks(current, comments.next_after, "Comments")); panel.append(discussion);
    if (editable(repository)) panel.append(editor({ repository, signal, title: "Join the discussion", comment: true,
      send: payload => api(`${path}/comments`, { method: "POST", body: payload, signal }),
      published: result => {
        // Comment numbers are repository-wide. Start immediately before this
        // confirmed post so it is visible even beyond the first comment page.
        navigate({ ...current, after: String(result.number - 1) }); notice("Comment added.");
      } }));
    else panel.append(element("p", "Connect with a write-scoped token to join the discussion.", "hint"));
    return panel;
  }

  return (repository, current, signal) => current.view === "new-issue" ? newIssue(repository, current, signal) :
    current.view === "issue" ? detail(repository, current, signal) : list(repository, current, signal);
})();
