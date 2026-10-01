import { cleanup, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { CatalogTree } from "./CatalogTree";
import { emptyCatalog, type CatalogObject } from "../lib/catalog";

const model: CatalogObject = {
  id: "detector",
  name: "detector",
  namespace: "vql.quality",
  kind: "MODEL",
  lookupNames: ["quality.detector"],
  ddl: "",
  problem: null,
  versions: [
    { name: "release-1", isDefault: false },
    { name: "v2", isDefault: true },
  ],
};
function props(object = model) {
  return {
    objects: { ...emptyCatalog(), models: [object], functions: [object] },
    connected: true,
    loading: false,
    busy: false,
    problem: null,
    activeSection: "models" as const,
    activeNamespace: "vql.quality",
    selectedId: model.id,
    onRefresh: vi.fn(),
    onNavigate: vi.fn(),
    onSelect: vi.fn(),
  };
}
afterEach(cleanup);

describe("Catalog object navigation", () => {
  it("keeps multi-version Models as object leaves in Models and Functions", async () => {
    const options = props();
    render(<CatalogTree {...options} />);
    const models = screen.getByRole("list", { name: "vql.quality models" });
    const selected = within(models).getByRole("button", {
      name: "Show DDL for model vql.quality.detector",
    });
    expect(selected).toHaveAttribute("aria-current", "true");
    expect(
      screen.queryByRole("list", { name: /Versions of model/ }),
    ).toBeNull();
    expect(screen.queryByText("Default")).toBeNull();
    expect(screen.queryByText("release-1")).toBeNull();
    await userEvent.click(selected);
    expect(options.onSelect).toHaveBeenLastCalledWith("models", model);
    const functions = screen.getByRole("list", {
      name: "vql.quality functions",
    });
    const callable = within(functions).getByRole("button", {
      name: "Show DDL for model vql.quality.detector",
    });
    expect(callable).not.toHaveAttribute("aria-current");
    await userEvent.click(callable);
    expect(options.onSelect).toHaveBeenLastCalledWith("functions", model);
  });

  it("retains collapsible categories without version children", async () => {
    render(<CatalogTree {...props()} />);
    const category = screen.getByRole("button", {
      name: "Models in vql.quality",
    });
    await userEvent.click(category);
    expect(
      screen.queryByRole("list", { name: "vql.quality models" }),
    ).toBeNull();
    await userEvent.click(category);
    expect(
      screen.getByRole("list", { name: "vql.quality models" }),
    ).toBeVisible();
    expect(
      screen.queryByRole("button", { name: /Versions of model/ }),
    ).toBeNull();
  });
});
