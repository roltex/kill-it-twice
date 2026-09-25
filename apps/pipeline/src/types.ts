export interface RecordRow {
  id: number;
  email: string;
  name: string;
  payload: Record<string, unknown>;
  version: number;
  poison: boolean;
  updated_at: Date;
  created_at: Date;
}

export interface Checkpoint {
  mode: 'backfill' | 'incremental';
  cursor_id: number;
  cursor_ts: Date | null;
  updated_at: Date;
}

export interface DlqRow {
  id: number;
  record_id: number;
  version: number;
  batch_id: string;
  sink: string;
  payload: Record<string, unknown>;
  error: string;
  status: string;
  created_at: Date;
  replayed_at: Date | null;
}

export function toDocument(row: RecordRow) {
  const updated = row.updated_at instanceof Date ? row.updated_at : new Date(row.updated_at);
  const created = row.created_at instanceof Date ? row.created_at : new Date(row.created_at);
  return {
    id: Number(row.id),
    email: row.email,
    name: row.name,
    payload: row.payload ?? {},
    version: Number(row.version),
    poison: Boolean(row.poison),
    updated_at: updated.toISOString(),
    created_at: created.toISOString(),
  };
}

export function eventId(recordId: number | string, version: number | string): string {
  return `${recordId}:${version}`;
}
