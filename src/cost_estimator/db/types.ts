export type TextColumnsMode = "auto" | "all";

export type ColumnInfo = {
  columnName: string;
  dataType: string;
  isNullable: boolean;
  columnDefault: string | null;
  ordinalPosition: number;
  pkPosition: number | null;
};

export type TableInfo = {
  schema: string;
  table: string;
  columns: ColumnInfo[];
  pkColumns: string[];
  updatedAtColumn: string | null;
};
