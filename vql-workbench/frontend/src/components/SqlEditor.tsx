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
import { sql, PostgreSQL, SQLDialect } from "@codemirror/lang-sql";
import {
  bracketMatching,
  HighlightStyle,
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
import { tags } from "@lezer/highlight";
import { forwardRef, useEffect, useImperativeHandle, useRef } from "react";

import { selectedOrCurrentStatement, type EditorSlice } from "../lib/sql";

const visionqlDialect = SQLDialect.define({
  ...PostgreSQL.spec,
  keywords: `${PostgreSQL.spec.keywords} model version default_version location options object_detection onnx_runtime images video rtsp kafka python tblproperties`,
  types: `${PostgreSQL.spec.types} image box2d vector`,
});
const sqlHighlightStyle = HighlightStyle.define([
  { tag: tags.keyword, class: "tok-keyword" },
  { tag: tags.typeName, class: "tok-type" },
  { tag: tags.function(tags.variableName), class: "tok-function" },
  { tag: tags.string, class: "tok-string" },
  { tag: tags.number, class: "tok-number" },
  { tag: tags.comment, class: "tok-comment" },
]);

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
    onChange?: (value: string) => void;
    onRun?: () => void;
    onRunCurrent?: () => void;
    disabled: boolean;
    readOnly?: boolean;
    ariaLabel?: string;
  }
>(
  (
    {
      value,
      onChange,
      onRun,
      onRunCurrent,
      disabled,
      readOnly = false,
      ariaLabel = "SQL editor",
    },
    ref,
  ) => {
    const parent = useRef<HTMLDivElement>(null);
    const viewRef = useRef<EditorView | null>(null);
    const mode = useRef(new Compartment());
    const callbacks = useRef({
      onChange,
      onRun,
      onRunCurrent,
      readOnly,
      disabled,
    });
    callbacks.current = { onChange, onRun, onRunCurrent, readOnly, disabled };
    const modeExtensions = () => [
      EditorState.readOnly.of(readOnly),
      EditorView.editable.of(!disabled),
      EditorView.contentAttributes.of({
        "aria-label": ariaLabel,
        "aria-readonly": String(readOnly),
        spellcheck: "false",
      }),
      ...(readOnly
        ? []
        : [
            history(),
            dropCursor(),
            closeBrackets(),
            autocompletion(),
            keymap.of([
              indentWithTab,
              ...closeBracketsKeymap,
              ...historyKeymap,
            ]),
          ]),
    ];

    useEffect(() => {
      if (!parent.current) return;
      const state = EditorState.create({
        doc: value,
        extensions: [
          lineNumbers(),
          highlightActiveLineGutter(),
          highlightSpecialChars(),
          drawSelection(),
          EditorState.allowMultipleSelections.of(true),
          bracketMatching(),
          rectangularSelection(),
          highlightActiveLine(),
          highlightSelectionMatches(),
          syntaxHighlighting(sqlHighlightStyle),
          sql({ dialect: visionqlDialect, upperCaseKeywords: true }),
          EditorView.lineWrapping,
          mode.current.of(modeExtensions()),
          EditorView.theme({
            "&": { height: "100%" },
            ".cm-content": { padding: "12px 0 28px" },
            ".cm-line": { padding: "0 18px" },
            ".cm-gutterElement": { padding: "0 12px 0 8px" },
            ".tok-keyword": { color: "#b53c0a", fontWeight: "600" },
            ".tok-function": { color: "#126fc1" },
            ".tok-type": { color: "#126fc1" },
            ".tok-string": { color: "#23825f" },
            ".tok-number": { color: "#f04b0f" },
            ".tok-comment": { color: "#908c83", fontStyle: "italic" },
          }),
          EditorView.updateListener.of((update) => {
            if (update.docChanged && !callbacks.current.readOnly) {
              callbacks.current.onChange?.(update.state.doc.toString());
            }
          }),
          keymap.of([
            {
              key: "Mod-Enter",
              run: () => {
                if (!callbacks.current.readOnly && !callbacks.current.disabled)
                  callbacks.current.onRun?.();
                return true;
              },
            },
            {
              key: "Shift-Mod-Enter",
              run: () => {
                if (!callbacks.current.readOnly && !callbacks.current.disabled)
                  callbacks.current.onRunCurrent?.();
                return true;
              },
            },
            ...defaultKeymap,
            ...searchKeymap,
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
        effects: mode.current.reconfigure(modeExtensions()),
      });
    }, [disabled, readOnly, ariaLabel]);

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

    return (
      <div ref={parent} className="h-full min-h-0 w-full overflow-hidden" />
    );
  },
);
SqlEditor.displayName = "SqlEditor";
