"use strict";

const discussion = (() => {
  const editable = repository => repository.viewer.token_scope !== "read";
  const owns = (repository, record) => editable(repository) &&
    (repository.role !== "read" || repository.viewer.account === record.author);
  const date = value => new Date(value).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });

  async function read(repository, path, signal) {
    const data = await api(path, { signal });
    if (data.repository_id !== repository.repository_id) throw new Error("Repository identity changed. Reload this page.");
    return data;
  }
  function pageLinks(current, next, label) {
    const pager = element("nav", undefined, "pager"); pager.setAttribute("aria-label", `${label} pages`);
    if (current.after) pager.append(link("First page", { ...current, after: undefined }));
    if (next) pager.append(link(`More ${label.toLowerCase()} →`, { ...current, after: String(next) }));
    return pager;
  }
  function field(form, name, label, value, limit, required = false) {
    const node = element(name === "title" ? "input" : "textarea");
    node.name = name; node.value = value; node.maxLength = limit; node.required = required;
    if (name !== "title") node.rows = 7;
    const wrapper = element("label", label); wrapper.append(node); form.append(wrapper);
    const validate = () => {
      const tooLong = new TextEncoder().encode(node.value).length > limit;
      const invalid = name === "title" ? /\p{Cc}/u.test(node.value) : node.value.includes("\0");
      node.setCustomValidity(tooLong ? `${label} must fit within ${limit.toLocaleString()} UTF-8 bytes.` :
        invalid ? `${label} contains unsupported control characters.` :
        node.required && !node.value.trim() ? `${label} cannot be empty.` : "");
    };
    node.addEventListener("input", validate); validate(); return node;
  }

  let sequence = 0;
  function select(form, label, choices, value) {
    const node = element("select"), caption = element("label", label);
    node.id = `discussion-choice-${++sequence}`; caption.htmlFor = node.id;
    for (const [key, text] of choices) { const option = element("option", text); option.value = key; node.append(option); }
    node.value = value; form.append(caption, node); return node;
  }
  function submit({ repository, form, signal, label, edit = false, payload: intent, send, published, cancel }) {
    const controls = Array.from(form.querySelectorAll("input,textarea,select"));
    form.append(element("p", controls.length ? "Your draft and pending submission stay on this page until you leave or reload." : "Your pending submission stays on this page until you leave or reload.", "hint"));
    const error = element("p", "", "error"); error.setAttribute("role", "alert");
    const actions = element("div", undefined, "actions");
    const save = element("button", label, "primary"); save.type = "submit";
    const reload = button("Reload current version", render); reload.hidden = true;
    if (cancel) actions.append(button("Cancel", cancel)); actions.append(reload, save); form.append(error, actions);
    const locked = value => { for (const control of controls) {
      if (control.tagName === "SELECT" || control.type === "checkbox") control.disabled = value;
      else control.readOnly = value;
    } };
    let payload = null, inFlight = false, uncertain = false;
    form.addEventListener("submit", async event => {
      event.preventDefault(); if (inFlight || signal.aborted || save.disabled) return;
      if (!form.reportValidity()) return;
      inFlight = true; save.disabled = true; locked(true); error.textContent = "";
      // Freeze identity and original bytes after an ambiguous reply. Even a
      // later permission rejection cannot prove the original write was absent.
      try {
        payload ||= { repository_id: repository.repository_id, ...(edit ? {} : { id: crypto.randomUUID() }), ...intent() };
        const result = await send(payload); signal.throwIfAborted(); published(result);
      } catch (failure) {
        if (signal.aborted) return;
        if (!uncertain && [400, 403, 404, 413, 422].includes(failure.status)) {
          payload = null; locked(false); save.disabled = false; error.textContent = failure.message;
        } else if (edit) {
          error.textContent = `${failure.message}\nYour changes were not confirmed. ${controls.length ? "Copy your draft, then reload the current version before editing again." : "Reload the current version before trying again."}`;
          reload.hidden = false;
        } else {
          uncertain = true;
          error.textContent = `${failure.message}\nWe couldn’t confirm the result. Retry the same submission. Check current state before starting another submission; leaving this page discards the pending identity.`;
          save.textContent = "Retry submission"; save.disabled = false; reload.hidden = false;
        }
      } finally { inFlight = false; }
    });
    return form;
  }
  return { editable, owns, date, read, pageLinks, field, select, submit };
})();
