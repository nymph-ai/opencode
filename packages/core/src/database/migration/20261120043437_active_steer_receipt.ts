import { Effect } from "effect"
import type { DatabaseMigration } from "../migration.js"

const migration: DatabaseMigration.Migration = {
  id: "20261120043437_active_steer_receipt",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`session_steer_receipt\` (
          \`id\` text PRIMARY KEY,
          \`session_id\` text NOT NULL,
          \`execution_id\` text NOT NULL,
          CONSTRAINT \`fk_session_steer_receipt_session_id_session_v2_id_fk\` FOREIGN KEY (\`session_id\`) REFERENCES \`session_v2\`(\`id\`) ON DELETE CASCADE
        );
      `)
      yield* tx.run(`CREATE INDEX \`session_steer_receipt_session_idx\` ON \`session_steer_receipt\` (\`session_id\`);`)
    })
  },
}

export default migration
