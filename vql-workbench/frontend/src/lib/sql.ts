export interface EditorSlice {
  sql: string;
  from: number;
  to: number;
}

export function selectedOrCurrentStatement(
  document: string,
  selectionFrom: number,
  selectionTo: number,
): EditorSlice {
  if (selectionFrom !== selectionTo) {
    return {
      sql: document.slice(selectionFrom, selectionTo).trim(),
      from: selectionFrom,
      to: selectionTo,
    };
  }
  const boundaries = statementBoundaries(document);
  const current = boundaries.find(
    ([from, to]) => selectionFrom >= from && selectionFrom <= to,
  );
  const [from, to] = current ?? [0, document.length];
  return { sql: document.slice(from, to).trim(), from, to };
}

export async function formatSql(sql: string): Promise<string> {
  const { format } = await import("sql-formatter");
  return format(sql, {
    language: "postgresql",
    keywordCase: "upper",
    dataTypeCase: "upper",
    functionCase: "preserve",
    tabWidth: 2,
    linesBetweenQueries: 1,
  });
}

export async function formatCatalogDdl(sql: string): Promise<string> {
  const { formatDialect, postgresql } = await import("sql-formatter");
  const clauses = [
    "CREATE MODEL",
    "TYPE",
    "VERSION",
    "FROM",
    "USING",
    "LOCATION",
    "OPTIONS",
    "RETURNS",
    "RETURN",
    "LANGUAGE PYTHON AS",
    "COMMENT",
    "TBLPROPERTIES",
  ];
  return formatDialect(sql, {
    dialect: {
      ...postgresql,
      name: "visionql-ddl",
      tokenizerOptions: {
        ...postgresql.tokenizerOptions,
        reservedClauses: [
          ...postgresql.tokenizerOptions.reservedClauses,
          ...clauses,
        ],
      },
      formatOptions: {
        ...postgresql.formatOptions,
        onelineClauses: [
          ...(postgresql.formatOptions.onelineClauses ?? []),
          ...clauses,
        ],
      },
    },
    keywordCase: "upper",
    dataTypeCase: "upper",
    tabWidth: 2,
    expressionWidth: 40,
  });
}

function statementBoundaries(sql: string): Array<[number, number]> {
  const result: Array<[number, number]> = [];
  let start = 0;
  let quote: "'" | '"' | "`" | null = null;
  let lineComment = false;
  let blockDepth = 0;
  for (let index = 0; index < sql.length; index += 1) {
    const current = sql[index];
    const next = sql[index + 1];
    if (lineComment) {
      if (current === "\n") lineComment = false;
      continue;
    }
    if (blockDepth > 0) {
      if (current === "/" && next === "*") {
        blockDepth += 1;
        index += 1;
      } else if (current === "*" && next === "/") {
        blockDepth -= 1;
        index += 1;
      }
      continue;
    }
    if (quote) {
      if (current === quote) {
        if (next === quote) index += 1;
        else quote = null;
      } else if (current === "\\" && quote !== "`") {
        index += 1;
      }
      continue;
    }
    if (current === "-" && next === "-") {
      lineComment = true;
      index += 1;
    } else if (current === "/" && next === "*") {
      blockDepth = 1;
      index += 1;
    } else if (current === "'" || current === '"' || current === "`") {
      quote = current;
    } else if (current === ";") {
      result.push([start, index + 1]);
      start = index + 1;
    }
  }
  if (sql.slice(start).trim()) result.push([start, sql.length]);
  return result.length ? result : [[0, sql.length]];
}
