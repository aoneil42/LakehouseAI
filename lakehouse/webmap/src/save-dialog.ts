/**
 * Modal dialog for saving a scratch layer to a permanent namespace/table.
 *
 * Fetches existing namespaces from /api/namespaces, filters out _scratch_*,
 * and presents a simple form with namespace + table name inputs.
 */

export interface SaveDialogResult {
  targetNamespace: string;
  targetTable: string;
}

export function showSaveDialog(
  sourceNamespace: string,
  sourceTable: string,
): Promise<SaveDialogResult | null> {
  return new Promise((resolve) => {
    const modal = document.createElement("div");
    modal.className = "save-dialog-modal";
    modal.innerHTML = `
      <div class="save-dialog-backdrop"></div>
      <div class="save-dialog-content">
        <h3>Save to Permanent Layer</h3>
        <div class="save-dialog-source">
          Source: <strong>${sourceNamespace}.${sourceTable}</strong>
        </div>
        <div class="save-dialog-row">
          <label>Namespace</label>
          <input type="text" id="save-ns-input" list="save-ns-list"
                 placeholder="Select or type new namespace" />
          <datalist id="save-ns-list"></datalist>
        </div>
        <div class="save-dialog-row">
          <label>Table Name</label>
          <input type="text" id="save-table-input"
                 value="${sourceTable}" />
        </div>
        <div class="save-dialog-error" id="save-error"></div>
        <div class="save-dialog-buttons">
          <button class="save-dialog-cancel">Cancel</button>
          <button class="save-dialog-save">Save</button>
        </div>
      </div>
    `;

    document.body.appendChild(modal);

    const nsInput = modal.querySelector("#save-ns-input") as HTMLInputElement;
    const nsList = modal.querySelector("#save-ns-list") as HTMLDataListElement;
    const tableInput = modal.querySelector("#save-table-input") as HTMLInputElement;
    const errorEl = modal.querySelector("#save-error") as HTMLElement;
    const cancelBtn = modal.querySelector(".save-dialog-cancel")!;
    const saveBtn = modal.querySelector(".save-dialog-save")!;
    const backdrop = modal.querySelector(".save-dialog-backdrop")!;

    // Populate namespace datalist
    fetch("/api/namespaces")
      .then((r) => r.json())
      .then((namespaces: string[]) => {
        const filtered = namespaces.filter(
          (ns) => !ns.startsWith("_scratch_"),
        );
        for (const ns of filtered) {
          const opt = document.createElement("option");
          opt.value = ns;
          nsList.appendChild(opt);
        }
        if (filtered.length > 0) {
          nsInput.value = filtered[0];
        }
      })
      .catch(() => {
        /* ignore fetch errors */
      });

    nsInput.focus();

    function close(result: SaveDialogResult | null) {
      modal.remove();
      resolve(result);
    }

    cancelBtn.addEventListener("click", () => close(null));
    backdrop.addEventListener("click", () => close(null));
    modal.addEventListener("keydown", (e) => {
      if (e.key === "Escape") close(null);
    });

    saveBtn.addEventListener("click", () => {
      const ns = nsInput.value.trim();
      const table = tableInput.value.trim();

      if (!ns) {
        errorEl.textContent = "Namespace is required";
        return;
      }
      if (!table) {
        errorEl.textContent = "Table name is required";
        return;
      }
      if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(ns)) {
        errorEl.textContent =
          "Namespace must start with a letter/underscore, only alphanumeric and underscores";
        return;
      }
      if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(table)) {
        errorEl.textContent =
          "Table name must start with a letter/underscore, only alphanumeric and underscores";
        return;
      }
      if (ns.startsWith("_scratch_")) {
        errorEl.textContent = "Target cannot be a scratch namespace";
        return;
      }

      close({ targetNamespace: ns, targetTable: table });
    });
  });
}
