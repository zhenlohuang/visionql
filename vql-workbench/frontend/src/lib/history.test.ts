import { beforeEach, describe, expect, it } from "vitest";

import { createDraft, loadDrafts, saveDrafts } from "./history";
import type { Draft } from "./types";

beforeEach(() => localStorage.clear());

describe("draft persistence", () => {
  it("restores every open draft, including edits beyond the twentieth tab", () => {
    const drafts: Draft[] = [];
    for (let index = 0; index < 25; index += 1) {
      drafts.push(createDraft(drafts, `SELECT ${index};`));
    }
    drafts[20] = { ...drafts[20], name: "important.sql", sql: "SELECT 42;" };
    saveDrafts(drafts);

    expect(loadDrafts()).toEqual(drafts);
  });
});
