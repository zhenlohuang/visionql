import { useEffect, useState } from "react";

import { formatCatalogDdl } from "../lib/sql";
import { SqlEditor } from "./SqlEditor";

export function FormattedDdl({ sql }: { sql: string }) {
  const [formatted, setFormatted] = useState({ source: sql, text: sql });
  useEffect(() => {
    let disposed = false;
    if (sql)
      void formatCatalogDdl(sql).then(
        (text) => {
          if (!disposed) setFormatted({ source: sql, text });
        },
        () => {
          if (!disposed) setFormatted({ source: sql, text: sql });
        },
      );
    return () => {
      disposed = true;
    };
  }, [sql]);
  const text =
    (formatted.source === sql ? formatted.text : sql) ||
    "Definition unavailable";
  return (
    <section
      role="region"
      aria-label="Formatted DDL"
      className="flex min-h-0 flex-1 flex-col overflow-hidden"
    >
      <SqlEditor
        value={text}
        disabled={false}
        readOnly
        ariaLabel="DDL editor"
      />
    </section>
  );
}
