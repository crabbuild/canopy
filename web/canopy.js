"use strict";
const $ = id => document.getElementById(id);
const bytes = text => Uint8Array.from(atob(text.replace(/-/g, "+").replace(/_/g, "/")), c => c.charCodeAt(0));
const encode = data => btoa(Array.from(data, b => String.fromCharCode(b)).join("")).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
const decode = text => new TextDecoder().decode(bytes(text));
const safe = text => text.replace(/[\x00-\x1f\x7f\u202a-\u202e\u2066-\u2069]/g, c => `\\u${c.charCodeAt(0).toString(16).padStart(4, "0")}`);
const element = (tag, text, className) => { const node = document.createElement(tag); if (text !== undefined) node.textContent = text; if (className) node.className = className; return node; };
const button = (text, action, className = "quiet") => { const node = element("button", text, className); node.type = "button"; node.addEventListener("click", action); return node; };
const hash = route => `#${encodeURIComponent(JSON.stringify(route))}`;
const link = (text, route, className) => { const node = element("a", text, className); node.href = hash(route); return node; };
let token = "", repositories = [], nextRepository = null, session = 0, generation = 0, viewController = null, noticeTimer;
const requests = new Set(), downloads = new Set();
function notice(message) { clearTimeout(noticeTimer); $("notice").textContent = message; $("notice").hidden = false; noticeTimer = setTimeout(() => { $("notice").hidden = true; }, 9000); }
function revokeDownloads() { for (const url of downloads) URL.revokeObjectURL(url); downloads.clear(); }
function disconnect() {
  session++; generation++; token = ""; clearTimeout(noticeTimer); $("notice").hidden = true; viewController?.abort(); for (const controller of requests) controller.abort();
  $("visibility-dialog")?.remove();
  $("account-dialog")?.remove(); $("account-nav").hidden = true;
  revokeDownloads(); repositories = []; nextRepository = null; $("repositories").replaceChildren(); $("view").replaceChildren();
  $("connect-form").reset(); $("workspace").hidden = true; $("login").hidden = false; $("disconnect").hidden = true;
  $("create-dialog").close(); $("create-form").reset(); $("create-error").textContent = ""; $("token").focus(); document.title = "Canopy · Repositories";
}
// Aborting a fetch cannot retract an already resolved response. Session epochs
// keep a late reply from restoring private content after logout or reconnect.
function currentSession(epoch) { if (epoch !== session) throw new DOMException("Session changed", "AbortError"); }
async function api(path, { method = "GET", body, signal } = {}) {
  const epoch = session, controller = new AbortController(); requests.add(controller);
  const abort = () => controller.abort(); signal?.addEventListener("abort", abort, { once: true });
  if (signal?.aborted) controller.abort();
  const timer = setTimeout(abort, 125000);
  try {
    const response = await fetch(path, { method, body: body === undefined ? undefined : JSON.stringify(body),
      headers: { ...(token ? { Authorization: `Bearer ${token}` } : {}), "Content-Type": "application/json" }, cache: "no-store", credentials: "omit", redirect: "error", signal: controller.signal });
    if (!response.ok) {
      const message = await response.text(); currentSession(epoch);
      if (response.status === 401) { const authenticated = Boolean(token); disconnect(); notice(authenticated ? "Your access token was rejected. Connect with an active token." : "Connect with an access token to read this repository."); }
      const error = new Error(`${message || "Request failed"} (${response.status})`); error.status = response.status; throw error;
    }
    const data = response.status === 204 ? null : await response.json(); currentSession(epoch);
    return data;
  } finally { clearTimeout(timer); requests.delete(controller); signal?.removeEventListener("abort", abort); }
}
function route() {
  try {
    const value = JSON.parse(decodeURIComponent(location.hash.slice(1)));
    if (value?.view === "accounts") return { view: "accounts", account: typeof value.account === "string" ? value.account : undefined, after: typeof value.after === "string" ? value.after : undefined };
    if (!value || typeof value.repo !== "string") return {};
    return { repo: value.repo, view: ["history", "file", "issues", "issue", "new-issue", "pulls", "pull", "new-pull"].includes(value.view) ? value.view : "tree",
      thread: Number.isSafeInteger(value.thread) && value.thread > 0 ? value.thread : undefined,
      review: Number.isSafeInteger(value.review) && value.review > 0 ? value.review : undefined,
      pull: Number.isSafeInteger(value.pull) && value.pull > 0 ? value.pull : undefined,
      section: ["changes", "merge", "threads"].includes(value.section) ? value.section : "discussion",
      candidate: typeof value.candidate === "string" ? value.candidate : undefined,
      issue: Number.isSafeInteger(value.issue) && value.issue > 0 ? value.issue : undefined,
      state: ["open", "closed", "merged", "all"].includes(value.state) ? value.state : "open",
      commit: typeof value.commit === "string" ? value.commit : undefined, path: typeof value.path === "string" ? value.path : "",
      reference: typeof value.reference === "string" ? value.reference : undefined,
      after: typeof value.after === "string" ? value.after : undefined };
  } catch { return {}; }
}
function navigate(value) { const next = hash(value); if (location.hash === next) render(); else location.hash = next; }
function sidebar() {
  const current = route().repo;
  $("repositories").replaceChildren(...repositories.map(repository => {
    const row = link("", { repo: repository.name }, "repo-link");
    row.append(element("span", "⑂", "repo-icon"), element("span", repository.name));
    if (repository.name === current) row.setAttribute("aria-current", "page");
    return row;
  }));
  $("more-repos").hidden = !nextRepository;
}
async function loadRepositories(append = false) {
  const epoch = session;
  const data = await api(`/api/repositories${append && nextRepository ? `?after=${encodeURIComponent(nextRepository)}` : ""}`);
  currentSession(epoch);
  repositories = append ? repositories.concat(data.repositories) : data.repositories;
  nextRepository = data.next_cursor; sidebar();
}
$("connect-form").addEventListener("submit", async event => {
  event.preventDefault(); const submit = event.currentTarget.querySelector("button"); submit.disabled = true;
  clearTimeout(noticeTimer); $("notice").hidden = true;
  const epoch = ++session; token = $("token").value.trim(); $("new-repo").hidden = false; $("disconnect").textContent = "Disconnect";
  try {
    await loadRepositories(); currentSession(epoch); $("connect-form").reset(); $("login").hidden = true; $("workspace").hidden = false; $("disconnect").hidden = false;
    $("account-nav").hidden = false; await render();
  } catch (error) { if (epoch === session) { disconnect(); notice(error.message); } } finally { submit.disabled = false; }
});
document.querySelector(".skip").addEventListener("click", event => { event.preventDefault(); $("content").focus(); });
$("disconnect").addEventListener("click", disconnect);
$("account-nav").addEventListener("click", () => navigate({ view: "accounts" }));
async function browsePublic() {
  disconnect(); const epoch = ++session;
  try {
    await loadRepositories(); currentSession(epoch);
    $("login").hidden = true; $("workspace").hidden = false;
    $("new-repo").hidden = true; $("disconnect").hidden = false; $("disconnect").textContent = "Connect";
    await render();
  } catch (error) { if (epoch === session) notice(error.message); }
}
$("browse-public").addEventListener("click", browsePublic);
window.addEventListener("DOMContentLoaded", () => { if (route().repo) browsePublic(); });
$("more-repos").addEventListener("click", async event => { event.currentTarget.disabled = true; try { await loadRepositories(true); } catch (error) { if (error.name !== "AbortError") notice(error.message); } finally { $("more-repos").disabled = false; } });
$("new-repo").addEventListener("click", () => { $("create-error").textContent = ""; $("create-dialog").showModal(); $("repo-name").focus(); });
$("cancel-create").addEventListener("click", () => $("create-dialog").close());
$("create-form").addEventListener("submit", async event => {
  event.preventDefault(); const submit = event.currentTarget.querySelector("[type=submit]"); submit.disabled = true;
  const epoch = session;
  try {
    const created = await api("/api/repositories", { method: "POST", body: { name: $("repo-name").value.trim() } });
    currentSession(epoch); $("create-dialog").close(); $("create-form").reset(); await loadRepositories(); currentSession(epoch); navigate({ repo: created.name }); notice("Repository created.");
  } catch (error) { if (epoch === session) $("create-error").textContent = error.message; } finally { submit.disabled = false; }
});
window.addEventListener("hashchange", render);
async function browse(repository, query, signal) {
  const result = await api(`/api/repositories/${encodeURIComponent(repository.name)}/browse`, { method: "POST", body: { repository_id: repository.repository_id, query }, signal });
  return result.view;
}
function empty(title, message) { const panel = element("section", undefined, "empty"); panel.append(element("h2", title), element("p", message)); return panel; }
function welcome() {
  const panel = element("section", undefined, "welcome");
  panel.append(element("p", token ? "Your workspace" : "Public repositories", "eyebrow"), element("h1", repositories.length ? "Choose a repository." : token ? "Make room for your next project." : "No public repositories yet."), element("p", repositories.length ? "Open a repository to browse its files and follow the history behind them." : token ? "Create a repository, then push your first commit with Git. Repositories shared with you will appear here too." : "Connect with an access token to open repositories shared with you."));
  $("view").replaceChildren(panel);
}
function heading(repository) {
  const head = element("div", undefined, "repo-heading"), title = element("div");
  title.append(element("p", repository.owner, "repo-owner"), element("h1", repository.name), element("span", repository.visibility, "badge"), element("span", `${repository.role} access`, "badge"));
  const clone = element("div", undefined, "clone"), input = element("input"); input.value = repository.clone_url; input.readOnly = true; input.setAttribute("aria-label", "Clone URL");
  clone.append(input, button("Copy clone URL", async () => { try { await navigator.clipboard.writeText(repository.clone_url); notice("Clone URL copied."); } catch { input.style.display = "block"; input.focus(); input.select(); notice("Select and copy the clone URL."); } }));
  if (repository.role === "admin" && repository.viewer?.token_scope === "admin") {
    clone.append(button("Change visibility", () => visibilityDialog(repository)));
  }
  head.append(title, clone); return head;
}
async function visibilityDialog(repository) {
  const epoch = session, path = `/api/repositories/${encodeURIComponent(repository.name)}/visibility`;
  let current;
  try { current = await api(path); currentSession(epoch); } catch (error) { if (epoch === session) notice(error.message); return; }
  const dialog = element("dialog"), form = element("form"), select = element("select"), caption = element("label", "Who can read this repository?");
  for (const [value, text] of [["private", "Private — owner and collaborators"], ["public", "Public — anyone"]]) { const option = element("option", text); option.value = value; select.append(option); }
  $("visibility-dialog")?.remove(); dialog.id = "visibility-dialog"; dialog.setAttribute("aria-label", `Visibility of ${repository.name}`);
  select.value = current.visibility; caption.append(select);
  const error = element("p", "", "error"); error.setAttribute("role", "alert");
  const save = element("button", "Save visibility", "primary"); save.type = "submit";
  const close = () => { dialog.close(); dialog.remove(); };
  dialog.addEventListener("close", () => dialog.remove());
  const actions = element("div", undefined, "actions"); actions.append(button("Cancel", close), save);
  form.append(element("h2", `Visibility of ${repository.name}`), caption,
    element("p", "Public repositories expose code, LFS files, issues, pull requests and checks. Making a repository private cannot recall copies already downloaded.", "hint"), error,
    actions);
  form.addEventListener("submit", async event => {
    event.preventDefault(); save.disabled = true;
    try {
      await api(path, { method: "PUT", body: { repository_id: repository.repository_id, expected_generation: current.generation, visibility: select.value } });
      currentSession(epoch); close(); await loadRepositories(); await render(); notice("Repository visibility updated.");
    } catch (failure) { if (epoch === session) error.textContent = `${failure.message} Close and reopen this dialog to read current visibility.`; }
    finally { save.disabled = false; }
  });
  dialog.append(form); document.body.append(dialog); dialog.showModal(); select.focus();
}
function tabs(current, commit) {
  const node = element("nav", undefined, "tabs"); node.setAttribute("aria-label", "Repository views");
  for (const [text, view] of [["Files", "tree"], ["History", "history"], ["Issues", "issues"], ["Pull requests", "pulls"]]) {
    const tab = link(text, { repo: current.repo, view, commit, reference: current.reference }, "tab");
    if (view === current.view || (view === "tree" && current.view === "file") || (view === "issues" && ["issue", "new-issue"].includes(current.view)) || (view === "pulls" && ["pull", "new-pull"].includes(current.view))) tab.setAttribute("aria-current", "page");
    node.append(tab);
  }
  return node;
}
async function toolbar(repository, current, resolved, signal, ticket) {
  const node = element("div", undefined, "toolbar"), select = element("select"); select.setAttribute("aria-label", "Branch or tag");
  let after = null, refGeneration = null;
  const seen = new Set();
  const add = (name, label) => { if (seen.has(name)) return; seen.add(name); const option = element("option", safe(label)); option.value = name; select.append(option); };
  add(resolved.reference, resolved.reference.replace(/^refs\/(heads|tags)\//, "")); select.value = resolved.reference;
  select.addEventListener("change", () => navigate({ repo: repository.name, reference: select.value }));
  const more = button("More refs", async () => { more.disabled = true; try { await load(); } catch (error) { if (!signal.aborted) notice(error.message); } finally { more.disabled = false; } }); more.hidden = true;
  async function load() {
    const { refs } = await browse(repository, { kind: "refs", after, generation: refGeneration }, signal);
    if (ticket !== generation) return;
    refGeneration = refs.generation; after = refs.next_after;
    for (const reference of refs.entries) add(reference.name, reference.name.replace(/^refs\/(heads|tags)\//, ""));
    more.hidden = !after;
  }
  node.append(select, more, button("Refresh branch", () => navigate({ repo: repository.name, reference: resolved.reference })), element("span", current.commit ? `Snapshot ${current.commit.slice(0, 10)}` : "No commits yet", "snapshot"));
  await load(); return node;
}
function breadcrumbs(current) {
  const nav = element("nav", undefined, "breadcrumb"); nav.setAttribute("aria-label", "File path");
  nav.append(link(current.repo, { ...current, view: "tree", path: "", after: undefined }));
  if (!current.path) return nav;
  const raw = bytes(current.path); let start = 0;
  for (let i = 0; i <= raw.length; i++) {
    if (i < raw.length && raw[i] !== 47) continue;
    const part = raw.slice(start, i); start = i + 1;
    nav.append(element("span", "╱", "junction"));
    const label = safe(new TextDecoder().decode(part));
    if (i === raw.length) nav.append(element("span", label));
    else nav.append(link(label, { ...current, view: "tree", path: encode(raw.slice(0, i)), after: undefined }));
  }
  return nav;
}
const subject = commit => safe(decode(commit.message_base64).split("\n")[0] || "(No commit message)");
function authorLabel(commit) {
  if (!commit.author_base64) return "Unknown author";
  const raw = decode(commit.author_base64), identity = /^(.*) <([^>]*)> (-?\d+) ([+-]\d{4})$/.exec(raw);
  if (!identity) return safe(raw);
  const date = new Date(Number(identity[3]) * 1000);
  return `${safe(identity[1])}${Number.isNaN(date.valueOf()) ? "" : ` · ${date.toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" })}`}`;
}
function commitStrip(commit, current) {
  const strip = element("div", undefined, "commit-strip");
  strip.append(element("span", "●", "file-symbol"), link(subject(commit), { repo: current.repo, view: "history", commit: commit.oid, reference: current.reference }, "subject"), element("span", commit.author_base64 ? safe(decode(commit.author_base64).split(" <")[0]) : "Unknown author", "muted"), element("span", commit.oid.slice(0, 10), "hash"));
  return strip;
}
async function treeView(repository, current, signal) {
  const { tree } = await browse(repository, { kind: "tree", commit: current.commit, path_base64: current.path || "", after: current.after }, signal);
  const panel = element("div"), surface = element("section", undefined, "surface"); surface.append(commitStrip(tree.commit, current));
  const table = element("table", undefined, "files"), head = element("thead"), titles = element("tr");
  for (const title of ["Name", "Type"]) { const cell = element("th", title); cell.scope = "col"; titles.append(cell); }
  head.append(titles); table.append(head); const body = element("tbody");
  for (const entry of tree.entries) {
    const row = element("tr"), name = element("td"), target = link("", { ...current, view: entry.kind === "tree" ? "tree" : "file", path: entry.path_base64, after: undefined });
    const label = entry.name === null ? Array.from(bytes(entry.name_base64), byte => `\\x${byte.toString(16).padStart(2, "0")}`).join("") : safe(entry.name);
    target.append(element("span", entry.kind === "tree" ? "▱" : entry.kind === "symlink" ? "↗" : entry.kind === "gitlink" ? "⑂" : "≡", "file-symbol"), element("span", label, "filename")); target.title = label; name.append(target);
    row.append(name, element("td", entry.kind === "file" && entry.mode === "100755" ? "executable" : entry.kind, "type")); body.append(row);
  }
  table.append(body); surface.append(table); panel.append(surface);
  if (!tree.entries.length) panel.append(empty("This directory is empty.", "There are no entries in this snapshot."));
  if (tree.next_after || current.after) {
    const pager = element("div", undefined, "pager");
    if (current.after) pager.append(link("First page", { ...current, after: undefined }));
    if (tree.next_after) pager.append(link("Next files →", { ...current, after: tree.next_after })); panel.append(pager);
  }
  return panel;
}
async function fileView(repository, current, signal) {
  const { file } = await browse(repository, { kind: "file", commit: current.commit, path_base64: current.path }, signal);
  const surface = element("section", undefined, "surface"), meta = element("div", undefined, "file-meta");
  meta.append(element("span", `${file.size === null ? "Git submodule" : `${file.size.toLocaleString()} bytes`} · ${file.mode}`), element("span", file.oid.slice(0, 12), "hash")); surface.append(meta);
  if (file.content_status !== "included") { surface.append(element("p", file.content_status === "gitlink" ? "This entry points to a commit in another repository. Clone with submodules to read it." : "This file is larger than the 256 KiB preview limit. Clone the repository to read its full content.", "file-message")); return surface; }
  signal.throwIfAborted();
  const raw = bytes(file.content_base64), url = URL.createObjectURL(new Blob([raw], { type: "application/octet-stream" })); downloads.add(url);
  const download = element("a", "Download file"); download.href = url; download.download = file.path?.split("/").pop() || "file"; meta.append(download);
  let text;
  try { text = new TextDecoder("utf-8", { fatal: true }).decode(raw); } catch { text = null; }
  if (text === null || raw.includes(0)) surface.append(element("p", "Binary or non-UTF-8 content. Download the file to inspect the original bytes.", "file-message"));
  else {
    if (file.mode === "120000") surface.append(element("p", "Symbolic link target · shown as text", "file-message"));
    const pre = element("pre", text, "file-content"); pre.tabIndex = 0; pre.setAttribute("aria-label", "File content"); surface.append(pre);
  }
  return surface;
}
async function historyView(repository, current, signal) {
  const { history } = await browse(repository, { kind: "history", commit: current.commit }, signal);
  const panel = element("div"); panel.append(element("p", "First-parent history · choose a parent to follow another line", "hint"));
  const list = element("ol", undefined, "history surface");
  for (const commit of history.commits) {
    const row = element("li"), metadata = element("div", undefined, "metadata");
    row.append(link(subject(commit), { repo: current.repo, commit: commit.oid, reference: current.reference }, "subject"));
    metadata.append(element("span", authorLabel(commit)), element("span", commit.oid.slice(0, 10), "hash")); row.append(metadata);
    if (commit.message_truncated) row.append(element("p", "Commit message preview truncated at 4 KiB.", "hint"));
    if (commit.parents.length > 1) { const parents = element("div", undefined, "parents"); parents.append(element("span", "Parents")); for (const parent of commit.parents) parents.append(link(parent.slice(0, 10), { ...current, commit: parent, after: undefined }, "hash")); row.append(parents); }
    list.append(row);
  }
  panel.append(list);
  if (history.next_commit) { const pager = element("div", undefined, "pager"); pager.append(link("Earlier commits →", { ...current, commit: history.next_commit })); panel.append(pager); }
  return panel;
}
async function render() {
  if ($("workspace").hidden) return;
  const ticket = ++generation; viewController?.abort(); viewController = new AbortController(); const signal = viewController.signal;
  revokeDownloads(); sidebar(); const current = route(); $("view").replaceChildren(element("p", "Opening repository…", "loading"));
  if (!current.repo && current.view !== "accounts") { welcome(); return; }
  try {
    if (current.view === "accounts") {
      const panel = await accountsView(current, signal);
      if (ticket !== generation) return;
      $("view").replaceChildren(panel); document.title = "Accounts · Canopy"; return;
    }
    const repository = await api(`/api/repositories/${encodeURIComponent(current.repo)}`, { signal });
    const fragment = document.createDocumentFragment();
    fragment.append(heading(repository));
    if (["pulls", "pull", "new-pull"].includes(current.view)) {
      fragment.append(tabs(current), await pullsView(repository, current, signal));
    } else if (["issues", "issue", "new-issue"].includes(current.view)) {
      fragment.append(tabs(current), await issuesView(repository, current, signal));
    } else {
      const { resolved } = await browse(repository, { kind: "resolve", reference: current.reference }, signal);
      current.reference = resolved.reference; current.commit ||= resolved.oid;
      fragment.append(tabs(current, current.commit), await toolbar(repository, current, resolved, signal, ticket));
      if (!current.commit) fragment.append(empty("This reference has no commits yet.", "Copy the clone URL above to push a branch, or select another reference. Refresh the branch when you’re ready."));
      else {
        if (current.view !== "history") fragment.append(breadcrumbs(current));
        fragment.append(await (current.view === "history" ? historyView(repository, current, signal) : current.view === "file" ? fileView(repository, current, signal) : treeView(repository, current, signal)));
      }
    }
    if (ticket !== generation) return;
    $("view").replaceChildren(fragment); document.title = `${repository.name} · Canopy`;
  } catch (error) {
    if (ticket !== generation || signal.aborted) return;
    const panel = element("section", undefined, "error-panel"); panel.append(element("h2", "Could not open this view"), element("p", error.message, "error"), button("Try again", render), button("Return to repositories", () => navigate({})));
    $("view").replaceChildren(panel);
  }
}
