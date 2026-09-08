import {
  autocompletion,
  closeBrackets,
  closeBracketsKeymap,
} from "@codemirror/autocomplete";
import {
  defaultKeymap,
  history,
  historyKeymap,
  indentWithTab,
} from "@codemirror/commands";
import { sql, PostgreSQL } from "@codemirror/lang-sql";
import {
  bracketMatching,
  defaultHighlightStyle,
  syntaxHighlighting,
} from "@codemirror/language";
import { highlightSelectionMatches, searchKeymap } from "@codemirror/search";
import { Compartment, EditorState } from "@codemirror/state";
import {
  drawSelection,
  dropCursor,
  EditorView,
  highlightActiveLine,
  highlightActiveLineGutter,
  highlightSpecialChars,
  keymap,
  lineNumbers,
  rectangularSelection,
} from "@codemirror/view";
import { forwardRef, useEffect, useImperativeHandle, useRef } from "react";

import { selectedOrCurrentStatement, type EditorSlice } from "../lib/sql";

export interface SqlEditorHandle {
  selectionOrCurrent: () => EditorSlice;
  selection: () => EditorSlice;
  replace: (value: string) => void;
  focus: () => void;
}

export const SqlEditor = forwardRef<
  SqlEditorHandle,
  {
    value: string;
    onChange: (value: string) => void;
    onRun: () => void;
    onRunCurrent: () => void;
    disabled: boolean;
  }
>(({ value, onChange, onRun, onRunCurrent, disabled }, ref) => {
  const parent = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  const editable = useRef(new Compartment());
  const callbacks = useRef({ onChange, onRun, onRunCurrent });
  callbacks.current = { onChange, onRun, onRunCurrent };

  useEffect(() => {
    if (!parent.current) return;
    const state = EditorState.create({
      doc: value,
      extensions: [
        lineNumbers(),
        highlightActiveLineGutter(),
        highlightSpecialChars(),
        history(),
        drawSelection(),
        dropCursor(),
        EditorState.allowMultipleSelections.of(true),
        bracketMatching(),
        closeBrackets(),
        autocompletion(),
        rectangularSelection(),
        highlightActiveLine(),
        highlightSelectionMatches(),
        syntaxHighlighting(defaultHighlightStyle, { fallback: true }),
        sql({ dialect: PostgreSQL, upperCaseKeywords: true }),
        EditorView.lineWrapping,
        editable.current.of(EditorView.editable.of(!disabled)),
        EditorView.contentAttributes.of({
          "aria-label": "SQL editor",
          spellcheck: "false",
        }),
        EditorView.theme({
          "&": { height: "100%" },
          ".cm-content": { padding: "12px 0 28px" },
          ".cm-line": { padding: "0 18px" },
          ".cm-gutterElement": { padding: "0 12px 0 8px" },
          ".tok-keyword": { color: "#b53c0a", fontWeight: "600" },
          ".tok-function": { color: "#126fc1" },
          ".tok-string": { color: "#23825f" },
          ".tok-number": { color: "#f04b0f" },
          ".tok-comment": { color: "#908c83", fontStyle: "italic" },
        }),
        EditorView.updateListener.of((update) => {
          if (update.docChanged) {
            callbacks.current.onChange(update.state.doc.toString());
          }
        }),
        keymap.of([
          {
            key: "Mod-Enter",
            run: () => {
              callbacks.current.onRun();
              return true;
            },
          },
          {
            key: "Shift-Mod-Enter",
            run: () => {
              callbacks.current.onRunCurrent();
              return true;
            },
          },
          indentWithTab,
          ...closeBracketsKeymap,
          ...defaultKeymap,
          ...searchKeymap,
          ...historyKeymap,
        ]),
      ],
    });
    const view = new EditorView({ state, parent: parent.current });
    viewRef.current = view;
    return () => {
      view.destroy();
      viewRef.current = null;
    };
  }, []);

  useEffect(() => {
    const view = viewRef.current;
    if (!view || view.state.doc.toString() === value) return;
    view.dispatch({
      changes: { from: 0, to: view.state.doc.length, insert: value },
    });
  }, [value]);

  useEffect(() => {
    viewRef.current?.dispatch({
      effects: editable.current.reconfigure(EditorView.editable.of(!disabled)),
    });
  }, [disabled]);

  useImperativeHandle(ref, () => ({
    selectionOrCurrent: () => {
      const view = viewRef.current;
      if (!view) return { sql: value, from: 0, to: value.length };
      const range = view.state.selection.main;
      return selectedOrCurrentStatement(
        view.state.doc.toString(),
        range.from,
        range.to,
      );
    },
    selection: () => {
      const view = viewRef.current;
      if (!view) return { sql: "", from: 0, to: 0 };
      const range = view.state.selection.main;
      return {
        sql: view.state.doc.sliceString(range.from, range.to),
        from: range.from,
        to: range.to,
      };
    },
    replace: (nextValue) => {
      const view = viewRef.current;
      if (!view) return;
      view.dispatch({
        changes: { from: 0, to: view.state.doc.length, insert: nextValue },
      });
    },
    focus: () => viewRef.current?.focus(),
  }));

  return <div ref={parent} className="h-full min-h-0 w-full overflow-hidden" />;
});
SqlEditor.displayName = "SqlEditor";
