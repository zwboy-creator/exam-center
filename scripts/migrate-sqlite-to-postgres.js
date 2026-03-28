/**
 * 将本地 SQLite（app.db）整库复制到 PostgreSQL（覆盖云端同结构数据）
 * 先停止网站。用法:
 *   PowerShell: $env:DATABASE_URL="postgresql://..."; node scripts/migrate-sqlite-to-postgres.js
 * 可选: node scripts/migrate-sqlite-to-postgres.js [app.db 绝对或相对路径]
 */
const path = require("path");
const { DatabaseSync } = require("node:sqlite");
const { loadState: loadSqliteState } = require("../src/db/sqlite-store");
const { initPostgresStore, saveState } = require("../src/db/postgres-store");

const databaseUrl = String(process.env.DATABASE_URL || "").trim();
if (!databaseUrl) {
  console.error("请设置 DATABASE_URL（目标 PostgreSQL 连接串）");
  process.exit(1);
}

const sqlitePath = path.resolve(process.argv[2] || path.join(__dirname, "..", "src", "data", "app.db"));
let sql;
try {
  sql = new DatabaseSync(sqlitePath);
} catch (e) {
  console.error("无法打开 SQLite 文件:", sqlitePath, e.message);
  process.exit(1);
}

const db = loadSqliteState(sql);
sql.close();

if (!db) {
  console.error("SQLite 中无 meta 行，无法迁移");
  process.exit(1);
}

const noJson = path.join(__dirname, ".__skip_json_migrate__");

async function main() {
  const store = await initPostgresStore(databaseUrl, noJson);
  await saveState(store.pool, db);
  await store.pool.end();
  console.log(
    `迁移完成：用户 ${db.users.length}，考试记录 ${db.attempts.length}，题目 ${db.questions.length}`
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
