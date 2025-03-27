export interface ExportDatabaseData {
  data: Record<string, object[]>;
  schemaHash?: string;
  url?: string;
}

export interface ExportPgDataStructure {
  data: Record<string, object[]>;
  mode: 'pglite' | 'postgres';
  schemaHash: string;
}
