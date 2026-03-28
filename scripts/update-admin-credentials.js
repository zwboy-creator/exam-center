/**
 * 更新首位管理员（按 id 最小）的手机号与密码
 * 用法: node scripts/update-admin-credentials.js <手机号> <密码>
 * 若设置了 DATABASE_URL 则改 PostgreSQL，否则改本地 SQLite（src/data/app.db）
 */
const path = require("path");
const bcrypt = require("bcryptjs");
const { DatabaseSync } = require("node:sqlite");
const { Pool } = require("pg");

const phone = String(process.argv[2] || "").trim();
const password = String(process.argv[3] || "");

if (!phone || !password) {
  console.error("用法: node scripts/update-admin-credentials.js <手机号> <密码>");
  process.exit(1);
}

const passwordHash = bcrypt.hashSync(password, 10);
const updatedAt = new Date().toISOString();

const databaseUrl = String(process.env.DATABASE_URL || "").trim();

async function runPostgres() {
  const noSsl = process.env.DATABASE_SSL === "0" || process.env.DATABASE_SSL === "false";
  const pool = new Pool({
    connectionString: databaseUrl,
    ssl: noSsl ? false : { rejectUnauthorized: false },
  });
  try {
    const adminR = await pool.query(
      "SELECT id, phone FROM users WHERE role = 'admin' ORDER BY id LIMIT 1"
    );
    if (!adminR.rows.length) {
      console.error("未找到 role=admin 的用户");
      process.exit(1);
    }
    const admin = adminR.rows[0];
    const taken = await pool.query("SELECT id FROM users WHERE phone = $1 AND id != $2", [phone, admin.id]);
    if (taken.rows.length) {
      console.error("该手机号已被其他账号占用");
      process.exit(1);
    }
    await pool.query("UPDATE users SET phone = $1, password_hash = $2, updated_at = $3 WHERE id = $4", [
      phone,
      passwordHash,
      updatedAt,
      admin.id,
    ]);
    console.log(`已更新 PostgreSQL 管理员 id=${admin.id}，手机=${phone}`);
  } finally {
    await pool.end();
  }
}

function runSqlite() {
  const dbPath = path.join(__dirname, "..", "src", "data", "app.db");
  const sql = new DatabaseSync(dbPath);
  const admin = sql.prepare("SELECT id, phone FROM users WHERE role = 'admin' ORDER BY id LIMIT 1").get();
  if (!admin) {
    console.error("未找到 role=admin 的用户");
    process.exit(1);
  }
  const taken = sql.prepare("SELECT id FROM users WHERE phone = ? AND id != ?").get(phone, admin.id);
  if (taken) {
    console.error("该手机号已被其他账号占用");
    process.exit(1);
  }
  sql.prepare("UPDATE users SET phone = ?, password_hash = ?, updated_at = ? WHERE id = ?").run(
    phone,
    passwordHash,
    updatedAt,
    admin.id
  );
  sql.close();
  console.log(`已更新 SQLite 管理员 id=${admin.id}，手机=${phone}`);
}

if (databaseUrl) {
  runPostgres().catch((e) => {
    console.error(e);
    process.exit(1);
  });
} else {
  runSqlite();
}
