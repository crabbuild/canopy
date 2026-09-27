"use strict";
const accountsView = (() => {
  const accountPath = account => `/api/accounts/${encodeURIComponent(account)}`;
  const date = value => value === null ? "Never" : new Date(value).toLocaleString();
  function field(caption, input) { const label = element("label", caption); label.append(input); return label; }
  function select(values) {
    const node = element("select");
    for (const [value, text] of values) { const option = element("option", text); option.value = value; node.append(option); }
    return node;
  }
  function dialog(title) {
    const node = element("dialog"), form = element("form"), error = element("p", "", "error");
    node.id = "account-dialog"; node.setAttribute("aria-label", title); error.setAttribute("role", "alert");
    node.append(form); form.append(element("h2", title)); document.body.append(node);
    node.addEventListener("close", () => node.remove());
    return { node, form, error };
  }
  function issue(account, creating = false) {
    const epoch = session, { node, form, error } = dialog(creating ? "Create an account" : `Issue token for ${account}`);
    const name = element("input"); name.required = true; name.maxLength = 64; name.pattern = "[a-z0-9_.\\-]+"; name.autocomplete = "off"; name.spellcheck = false;
    const scope = select([["write", "Write — read and push code"], ["read", "Read — browse and clone"], ["admin", "Admin — manage credentials and permitted settings"]]);
    const lifetime = select([["30", "30 days"], ["7", "7 days"], ["90", "90 days"], ["", "No expiry"]]);
    const secret = element("textarea"); secret.readOnly = true; secret.rows = 3; secret.spellcheck = false; secret.autocomplete = "off";
    const result = element("section", undefined, "issued-token"); result.hidden = true;
    const state = element("p"); state.setAttribute("role", "status");
    result.append(state, field("Save this access token", secret), button("Copy token", async () => {
      try { await navigator.clipboard.writeText(secret.value); if (epoch === session) notice("Token copied. Store it somewhere safe."); }
      catch { secret.focus(); secret.select(); if (epoch === session) notice("Select and copy the token."); }
    }), element("p", "This secret is shown only here. Closing this dialog clears it from this tab. Canopy cannot show it again.", "hint"));
    const submit = element("button", creating ? "Create account" : "Issue token", "primary"); submit.type = "submit";
    let payload = null, busy = false, applied = false;
    const close = button("Cancel", () => {
      if (busy) return;
      const destination = creating && applied ? { view: "accounts", account: payload.name } : route();
      secret.value = ""; payload = null; node.close(); navigate(destination);
    });
    node.addEventListener("cancel", event => { if (payload || busy) event.preventDefault(); });
    const actions = element("div", undefined, "actions"); actions.append(close, submit);
    if (creating) form.append(field("Account name", name), element("p", "Lowercase letters, numbers, dots, underscores and hyphens. Disabled names stay reserved.", "hint"));
    form.append(field("Token scope", scope), element("p", "Scope limits what this token can do. Repository permissions still apply. Admin tokens can manage their own account’s credentials.", "hint"));
    if (creating) form.append(element("p", "The first token has no expiry. After connecting, use an admin token or ask the site administrator to issue expiring replacements.", "hint"));
    else form.append(field("Expires after", lifetime));
    form.append(error, result, actions);
    form.addEventListener("submit", async event => {
      event.preventDefault(); if (busy || applied) return;
      // A lost reply may follow a durable commit. Keep the secret, ID and expiry
      // fixed so retry recovers that credential instead of issuing another one.
      if (!payload) {
        try {
          const token = `cnp_${Array.from(crypto.getRandomValues(new Uint8Array(32)), byte => byte.toString(16).padStart(2, "0")).join("")}`;
          payload = creating ? { name: name.value.trim(), token, scope: scope.value } : { id: crypto.randomUUID(), token, scope: scope.value, expires_at_ms: lifetime.value ? Date.now() + Number(lifetime.value) * 86400000 : null };
        } catch { error.textContent = "Secure token generation is unavailable. Open Canopy over HTTPS or localhost."; return; }
        name.disabled = true; scope.disabled = true; lifetime.disabled = true;
        secret.value = payload.token; result.hidden = false; close.textContent = "I saved the token · Close";
      }
      busy = true; submit.disabled = true; close.disabled = true; error.textContent = "";
      state.textContent = "Saving credential… Keep this dialog open until the result is known.";
      try {
        await api(creating ? "/api/accounts" : `${accountPath(account)}/tokens`, { method: "POST", body: payload });
        currentSession(epoch); applied = true; submit.hidden = true;
        state.textContent = creating ? `Account ${payload.name} created. Save its first token.` : "Token issued. Save it before closing.";
      } catch (failure) {
        if (epoch !== session) return;
        state.textContent = "Issuance was not confirmed. Save this secret if you close; the request may have reached Canopy.";
        error.textContent = `${failure.message} Retry sends the same credential without issuing another one.`;
        submit.textContent = creating ? "Retry account creation" : "Retry token issuance";
      } finally { busy = false; submit.disabled = false; close.disabled = false; }
    });
    node.showModal(); (creating ? name : scope).focus();
  }
  function confirm(title, message, action, done) {
    const epoch = session, { node, form, error } = dialog(title);
    const submit = element("button", title, "danger-action"); submit.type = "submit";
    const cancel = button("Cancel", () => node.close()), actions = element("div", undefined, "actions"); actions.append(cancel, submit);
    let busy = false;
    node.addEventListener("cancel", event => { if (busy) event.preventDefault(); });
    form.append(element("p", message), error, actions);
    form.addEventListener("submit", async event => {
      event.preventDefault(); if (busy) return; busy = true; submit.disabled = true; cancel.disabled = true;
      try { await action(); currentSession(epoch); node.close(); await done(); }
      catch (failure) { if (epoch === session) error.textContent = `${failure.message} Refresh the view to check current state before retrying.`; }
      finally { busy = false; submit.disabled = false; cancel.disabled = false; }
    });
    node.showModal(); cancel.focus();
  }
  function pager(current, next) {
    const node = element("nav", undefined, "pager"); node.setAttribute("aria-label", "Account pagination");
    if (current.after) node.append(link("First page", { ...current, after: undefined }));
    if (next) node.append(link("Next page →", { ...current, after: next }));
    return node;
  }
  async function tokens(identity, current, signal) {
    const account = current.account || identity.account, path = `${accountPath(account)}/tokens`;
    const data = await api(`${path}${current.after ? `?after=${encodeURIComponent(current.after)}` : ""}`, { signal }); signal.throwIfAborted();
    const panel = element("section", undefined, "account-view"), heading = element("div", undefined, "account-heading"), title = element("div");
    title.append(element("p", account === identity.account ? "Your credentials" : "Account credentials", "eyebrow"), element("h1", account), element("p", "Access tokens", "muted"));
    const actions = element("div", undefined, "actions"); actions.append(button("Refresh", render), button("Issue token", () => issue(account), "primary")); heading.append(title, actions);
    panel.append(heading, element("p", "Up to 64 active tokens and 256 new credentials per rolling 24 hours. Revoked and expired IDs remain in the history.", "hint"));
    const list = element("ul", undefined, "credential-list surface");
    for (const credential of data.tokens) {
      const row = element("li"), details = element("div"), labels = element("div", undefined, "credential-labels");
      const expired = credential.expires_at_ms !== null && credential.expires_at_ms <= Date.now();
      const status = !credential.enabled ? "Revoked" : expired ? "Expired" : "Active";
      labels.append(element("strong", credential.scope), element("span", status, "badge"));
      const currentToken = account === identity.account && credential.id === identity.token_id;
      if (currentToken) labels.append(element("span", "This session", "badge current-token"));
      details.append(labels, element("code", credential.id, "credential-id"), element("p", `Created ${date(credential.created_at_ms)} · Expires ${date(credential.expires_at_ms)}`, "credential-date"));
      row.append(details);
      if (credential.enabled) row.append(button("Revoke", () => confirm("Revoke token", currentToken ? "This is the token for your current session. Revoking it disconnects this tab. Keep another active credential before proceeding." : `Revoke token ${credential.id}? New requests using it will be rejected.`, () => api(`${path}/${credential.id}`, { method: "DELETE" }), async () => {
        if (currentToken) { disconnect(); notice("Token revoked. Connect with another active token."); }
        else { await render(); notice("Token revoked."); }
      }), "quiet danger-action"));
      list.append(row);
    }
    panel.append(list);
    if (!data.tokens.length) panel.append(element("p", "No credentials on this page.", "muted"));
    panel.append(pager(current, data.next_after)); return panel;
  }
  async function audit(current, signal) {
    const data = await api(`/api/audit/accounts${current.after ? `?before=${encodeURIComponent(current.after)}` : ""}`, { signal }); signal.throwIfAborted();
    const panel = element("section", undefined, "account-view"), heading = element("div", undefined, "account-heading"), title = element("div");
    title.append(element("p", "Site administration", "eyebrow"), element("h1", "Account history"));
    heading.append(title, button("Refresh", render)); panel.append(heading, element("p", "Committed account and credential changes, newest first. Unchanged retries and rejected requests do not add entries. Token secrets are never recorded here.", "hint"));
    const actions = { "account.created": "Account created", "account.disabled": "Account disabled", "token.issued": "Token issued", "token.revoked": "Token revoked" };
    const list = element("ol", undefined, "credential-list surface"); list.setAttribute("aria-label", "Account history");
    for (const event of data.events) {
      const row = element("li"), details = element("div"), labels = element("div", undefined, "credential-labels");
      labels.append(element("strong", actions[event.action]), element("span", event.account, "badge"));
      details.append(labels, element("p", `${date(event.occurred_at_ms)} · ${event.actor === null ? "System bootstrap" : `By ${event.actor}`} · Event ${event.id}`, "credential-date"));
      if (event.actor_token_id) details.append(element("p", `Actor credential: ${event.actor_token_id}`, "credential-id"));
      if (event.token_id) details.append(element("code", `Target credential: ${event.token_id}`, "credential-id"), element("p", `${event.scope} scope · Expires ${date(event.expires_at_ms)}`, "credential-date"));
      row.append(details); list.append(row);
    }
    panel.append(list);
    if (!data.events.length) panel.append(element("p", "No account changes on this page.", "muted"));
    const navigation = element("nav", undefined, "pager"); navigation.setAttribute("aria-label", "History pagination");
    if (current.after) navigation.append(link("Newest changes", { view: "accounts", section: "audit" }));
    if (data.next_before) navigation.append(link("Older changes →", { view: "accounts", section: "audit", after: data.next_before }));
    panel.append(navigation); return panel;
  }
  return async (current, signal) => {
    const identity = await api("/api/session", { signal }); signal.throwIfAborted();
    const panel = element("div"), navigation = element("nav", undefined, "tabs"); navigation.setAttribute("aria-label", "Account views");
    if (identity.site_admin) {
      const all = link("All accounts", { view: "accounts" }, "tab");
      if (!current.account && current.section !== "audit") all.setAttribute("aria-current", "page"); navigation.append(all);
      const history = link("Account history", { view: "accounts", section: "audit" }, "tab");
      if (current.section === "audit") history.setAttribute("aria-current", "page"); navigation.append(history);
    }
    const mine = link("My tokens", { view: "accounts", account: identity.account }, "tab");
    if (current.account === identity.account || !identity.site_admin) mine.setAttribute("aria-current", "page");
    navigation.append(mine); panel.append(navigation);
    if (identity.token_scope !== "admin") {
      panel.append(empty(identity.account, `You are connected with a ${identity.token_scope} token. Connect with an admin token to manage credentials, or ask your site administrator.`)); return panel;
    }
    if (current.section === "audit") {
      panel.append(identity.site_admin ? await audit(current, signal) : empty("Account history", "Only the site administrator can read account history.")); return panel;
    }
    if (current.account || !identity.site_admin) { panel.append(await tokens(identity, current, signal)); return panel; }
    const data = await api(`/api/accounts${current.after ? `?after=${encodeURIComponent(current.after)}` : ""}`, { signal }); signal.throwIfAborted();
    const heading = element("div", undefined, "account-heading"), title = element("div"), actions = element("div", undefined, "actions");
    title.append(element("p", "Site administration", "eyebrow"), element("h1", "Accounts"));
    actions.append(button("Refresh", render), button("Create account", () => issue(null, true), "primary")); heading.append(title, actions); panel.append(heading);
    const list = element("ul", undefined, "credential-list surface");
    for (const account of data.accounts) {
      const row = element("li"), details = element("div"), labels = element("div", undefined, "credential-labels");
      labels.append(element("strong", account.name), element("span", account.enabled ? "Enabled" : "Disabled", "badge"));
      if (account.name === identity.account) labels.append(element("span", "Site owner", "badge current-token"));
      details.append(labels); row.append(details);
      if (account.enabled) {
        const actions = element("div", undefined, "actions"); actions.append(link("Manage tokens", { view: "accounts", account: account.name }));
        if (account.name !== identity.account) actions.append(button("Disable", () => confirm("Disable account", `Disable ${account.name}? All its tokens will be rejected for new requests. Existing data and permissions remain; re-enabling accounts is not available.`, () => api(`${accountPath(account.name)}/disable`, { method: "POST" }), async () => { await render(); notice("Account disabled."); }), "quiet danger-action"));
        row.append(actions);
      }
      list.append(row);
    }
    panel.append(list, pager(current, data.next_after)); return panel;
  };
})();
