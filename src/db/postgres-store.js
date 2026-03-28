/**
 * PostgreSQL 持久化（Neon / Supabase / Render Postgres 等，连接串 DATABASE_URL）
 * 与 sqlite-store 相同的内存 db 对象及整库 save 语义
 */
const fs = require("fs");
const { Pool } = require("pg");

const DEFAULT_SETTINGS = { examDurationMinutes: 60, passScore: 80, maxAttempts: 2 };

function nowIso() {
  return new Date().toISOString();
}

function emptyDbObject() {
  return {
    meta: { nextUserId: 1, nextAttemptId: 1, initializedAt: nowIso(), updatedAt: nowIso() },
    settings: { ...DEFAULT_SETTINGS },
    stores: [],
    departments: [],
    positions: [],
    storeDepartmentMap: {},
    departmentPositionMap: {},
    questions: [],
    users: [],
    attempts: [],
    logs: [],
  };
}

function createPool(databaseUrl) {
  const noSsl = process.env.DATABASE_SSL === "0" || process.env.DATABASE_SSL === "false";
  return new Pool({
    connectionString: databaseUrl,
    max: Number(process.env.PG_POOL_MAX || 5),
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 15000,
    ssl: noSsl ? false : { rejectUnauthorized: false },
  });
}

async function createSchema(pool) {
  const stmts = [
    `CREATE TABLE IF NOT EXISTS meta (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      next_user_id INTEGER NOT NULL DEFAULT 1,
      next_attempt_id INTEGER NOT NULL DEFAULT 1,
      initialized_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )`,
    `CREATE TABLE IF NOT EXISTS settings (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      exam_duration_minutes INTEGER NOT NULL DEFAULT 60,
      pass_score INTEGER NOT NULL DEFAULT 80,
      max_attempts INTEGER NOT NULL DEFAULT 2
    )`,
    `CREATE TABLE IF NOT EXISTS stores (id TEXT PRIMARY KEY, name TEXT NOT NULL)`,
    `CREATE TABLE IF NOT EXISTS departments (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`,
    `CREATE TABLE IF NOT EXISTS positions (id INTEGER PRIMARY KEY, name TEXT NOT NULL, category TEXT NOT NULL)`,
    `CREATE TABLE IF NOT EXISTS store_department (store_id TEXT NOT NULL, department_id INTEGER NOT NULL, PRIMARY KEY (store_id, department_id))`,
    `CREATE TABLE IF NOT EXISTS department_position (department_id INTEGER NOT NULL, position_id INTEGER NOT NULL, PRIMARY KEY (department_id, position_id))`,
    `CREATE TABLE IF NOT EXISTS questions (
      id INTEGER PRIMARY KEY,
      category TEXT NOT NULL,
      question_type TEXT NOT NULL,
      content TEXT NOT NULL,
      options_json TEXT NOT NULL,
      answer TEXT NOT NULL,
      score DOUBLE PRECISION NOT NULL DEFAULT 0
    )`,
    `CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      phone TEXT NOT NULL UNIQUE,
      password_hash TEXT NOT NULL,
      store_id TEXT NOT NULL,
      department_id INTEGER NOT NULL,
      position_id INTEGER NOT NULL,
      role TEXT NOT NULL,
      status TEXT NOT NULL,
      login_failures INTEGER NOT NULL DEFAULT 0,
      locked_until TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )`,
    `CREATE TABLE IF NOT EXISTS attempts (
      id INTEGER PRIMARY KEY,
      user_id INTEGER NOT NULL,
      attempt_no INTEGER NOT NULL,
      is_makeup BOOLEAN NOT NULL DEFAULT false,
      status TEXT NOT NULL,
      started_at TEXT,
      submitted_at TEXT,
      duration_minutes INTEGER NOT NULL,
      pass_score INTEGER NOT NULL,
      total_score DOUBLE PRECISION NOT NULL DEFAULT 0,
      raw_score DOUBLE PRECISION NOT NULL DEFAULT 0,
      pass BOOLEAN NOT NULL DEFAULT false,
      submitted_reason TEXT NOT NULL DEFAULT '',
      anti_cheat_json TEXT NOT NULL DEFAULT '{}',
      marked_question_ids_json TEXT NOT NULL DEFAULT '[]',
      questions_json TEXT NOT NULL DEFAULT '[]'
    )`,
    `CREATE TABLE IF NOT EXISTS logs (
      id SERIAL PRIMARY KEY,
      at TEXT NOT NULL,
      actor_id INTEGER,
      action TEXT NOT NULL,
      detail_json TEXT NOT NULL DEFAULT '{}'
    )`,
    `CREATE INDEX IF NOT EXISTS idx_attempts_user ON attempts(user_id)`,
    `CREATE INDEX IF NOT EXISTS idx_users_phone ON users(phone)`,
  ];
  for (const sql of stmts) await pool.query(sql);
}

async function loadState(pool) {
  const metaR = await pool.query("SELECT * FROM meta WHERE id = 1");
  if (!metaR.rows.length) return null;

  const metaRow = metaR.rows[0];
  const settingsR = await pool.query("SELECT * FROM settings WHERE id = 1");
  const settingsRow = settingsR.rows[0];
  const settings = settingsRow
    ? {
        examDurationMinutes: settingsRow.exam_duration_minutes,
        passScore: settingsRow.pass_score,
        maxAttempts: settingsRow.max_attempts,
      }
    : { ...DEFAULT_SETTINGS };

  const stores = (await pool.query("SELECT id, name FROM stores ORDER BY id")).rows;
  const departments = (await pool.query("SELECT id, name FROM departments ORDER BY id")).rows;
  const positions = (await pool.query("SELECT id, name, category FROM positions ORDER BY id")).rows;

  const storeDepartmentMap = {};
  for (const row of (await pool.query("SELECT store_id, department_id FROM store_department")).rows) {
    const k = String(row.store_id);
    if (!storeDepartmentMap[k]) storeDepartmentMap[k] = [];
    storeDepartmentMap[k].push(Number(row.department_id));
  }

  const departmentPositionMap = {};
  for (const row of (await pool.query("SELECT department_id, position_id FROM department_position")).rows) {
    const k = String(row.department_id);
    if (!departmentPositionMap[k]) departmentPositionMap[k] = [];
    departmentPositionMap[k].push(Number(row.position_id));
  }

  const questions = (await pool.query(
    'SELECT id, category, question_type AS "questionType", content, options_json AS "optionsJson", answer, score FROM questions ORDER BY id'
  )).rows.map((q) => ({
    id: q.id,
    category: q.category,
    questionType: q.questionType,
    content: q.content,
    options: JSON.parse(q.optionsJson || "[]"),
    answer: q.answer,
    score: q.score,
  }));

  const users = (await pool.query(
    `SELECT id, name, phone, password_hash AS "passwordHash", store_id AS "storeId", department_id AS "departmentId",
     position_id AS "positionId", role, status, login_failures AS "loginFailures", locked_until AS "lockedUntil",
     created_at AS "createdAt", updated_at AS "updatedAt" FROM users ORDER BY id`
  )).rows;

  const attemptRows = (await pool.query("SELECT * FROM attempts ORDER BY id")).rows;
  const attempts = attemptRows.map((a) => ({
    id: a.id,
    userId: a.user_id,
    attemptNo: a.attempt_no,
    isMakeup: Boolean(a.is_makeup),
    status: a.status,
    startedAt: a.started_at,
    submittedAt: a.submitted_at,
    durationMinutes: a.duration_minutes,
    passScore: a.pass_score,
    totalScore: a.total_score,
    rawScore: a.raw_score,
    pass: Boolean(a.pass),
    submittedReason: a.submitted_reason || "",
    antiCheat: JSON.parse(a.anti_cheat_json || "{}"),
    markedQuestionIds: JSON.parse(a.marked_question_ids_json || "[]"),
    questions: JSON.parse(a.questions_json || "[]"),
  }));

  const logs = (await pool.query(
    'SELECT at, actor_id AS "actorId", action, detail_json AS "detailJson" FROM logs ORDER BY id'
  )).rows.map((r) => ({
    at: r.at,
    actorId: r.actorId,
    action: r.action,
    detail: JSON.parse(r.detailJson || "{}"),
  }));

  return {
    meta: {
      nextUserId: metaRow.next_user_id,
      nextAttemptId: metaRow.next_attempt_id,
      initializedAt: metaRow.initialized_at,
      updatedAt: metaRow.updated_at,
    },
    settings,
    stores,
    departments,
    positions,
    storeDepartmentMap,
    departmentPositionMap,
    questions,
    users,
    attempts,
    logs,
  };
}

async function saveState(pool, db) {
  const t = nowIso();
  db.meta.updatedAt = t;

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    await client.query("DELETE FROM logs");
    await client.query("DELETE FROM attempts");
    await client.query("DELETE FROM users");
    await client.query("DELETE FROM questions");
    await client.query("DELETE FROM department_position");
    await client.query("DELETE FROM store_department");
    await client.query("DELETE FROM positions");
    await client.query("DELETE FROM departments");
    await client.query("DELETE FROM stores");

    await client.query(
      `INSERT INTO meta (id, next_user_id, next_attempt_id, initialized_at, updated_at) VALUES (1, $1, $2, $3, $4)
       ON CONFLICT (id) DO UPDATE SET next_user_id = EXCLUDED.next_user_id, next_attempt_id = EXCLUDED.next_attempt_id, updated_at = EXCLUDED.updated_at`,
      [db.meta.nextUserId, db.meta.nextAttemptId, db.meta.initializedAt || t, db.meta.updatedAt]
    );
    await client.query(
      `INSERT INTO settings (id, exam_duration_minutes, pass_score, max_attempts) VALUES (1, $1, $2, $3)
       ON CONFLICT (id) DO UPDATE SET exam_duration_minutes = EXCLUDED.exam_duration_minutes, pass_score = EXCLUDED.pass_score, max_attempts = EXCLUDED.max_attempts`,
      [db.settings.examDurationMinutes, db.settings.passScore, db.settings.maxAttempts]
    );

    for (const s of db.stores) {
      await client.query("INSERT INTO stores (id, name) VALUES ($1, $2)", [String(s.id), String(s.name)]);
    }
    for (const d of db.departments) {
      await client.query("INSERT INTO departments (id, name) VALUES ($1, $2)", [Number(d.id), String(d.name)]);
    }
    for (const p of db.positions) {
      await client.query("INSERT INTO positions (id, name, category) VALUES ($1, $2, $3)", [
        Number(p.id),
        String(p.name),
        String(p.category),
      ]);
    }

    const sdm = db.storeDepartmentMap || {};
    for (const storeId of Object.keys(sdm)) {
      for (const depId of sdm[storeId] || []) {
        await client.query("INSERT INTO store_department (store_id, department_id) VALUES ($1, $2)", [
          String(storeId),
          Number(depId),
        ]);
      }
    }

    const dpm = db.departmentPositionMap || {};
    for (const depId of Object.keys(dpm)) {
      for (const posId of dpm[depId] || []) {
        await client.query("INSERT INTO department_position (department_id, position_id) VALUES ($1, $2)", [
          Number(depId),
          Number(posId),
        ]);
      }
    }

    for (const q of db.questions) {
      await client.query(
        "INSERT INTO questions (id, category, question_type, content, options_json, answer, score) VALUES ($1, $2, $3, $4, $5, $6, $7)",
        [
          q.id,
          q.category,
          q.questionType,
          q.content,
          JSON.stringify(q.options || []),
          q.answer,
          Number(q.score) || 0,
        ]
      );
    }

    for (const u of db.users) {
      await client.query(
        `INSERT INTO users (id, name, phone, password_hash, store_id, department_id, position_id, role, status, login_failures, locked_until, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
        [
          u.id,
          u.name,
          u.phone,
          u.passwordHash,
          String(u.storeId),
          Number(u.departmentId),
          Number(u.positionId),
          u.role,
          u.status,
          Number(u.loginFailures || 0),
          u.lockedUntil || null,
          u.createdAt,
          u.updatedAt,
        ]
      );
    }

    for (const a of db.attempts) {
      await client.query(
        `INSERT INTO attempts (id, user_id, attempt_no, is_makeup, status, started_at, submitted_at, duration_minutes, pass_score, total_score, raw_score, pass, submitted_reason, anti_cheat_json, marked_question_ids_json, questions_json)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`,
        [
          a.id,
          a.userId,
          a.attemptNo,
          a.isMakeup,
          a.status,
          a.startedAt,
          a.submittedAt,
          a.durationMinutes,
          a.passScore,
          a.totalScore,
          a.rawScore,
          a.pass,
          a.submittedReason || "",
          JSON.stringify(a.antiCheat || {}),
          JSON.stringify(a.markedQuestionIds || []),
          JSON.stringify(a.questions || []),
        ]
      );
    }

    for (const L of db.logs || []) {
      await client.query("INSERT INTO logs (at, actor_id, action, detail_json) VALUES ($1, $2, $3, $4)", [
        L.at,
        L.actorId ?? null,
        L.action,
        JSON.stringify(L.detail || {}),
      ]);
    }

    await client.query("COMMIT");
  } catch (e) {
    try {
      await client.query("ROLLBACK");
    } catch (_) {
      /* 忽略 */
    }
    throw e;
  } finally {
    client.release();
  }
}

async function shouldMigrateFromJson(pool, jsonPath) {
  if (!fs.existsSync(jsonPath)) return false;
  const s = (await pool.query("SELECT COUNT(*)::int AS c FROM stores")).rows[0].c;
  const u = (await pool.query("SELECT COUNT(*)::int AS c FROM users")).rows[0].c;
  const q = (await pool.query("SELECT COUNT(*)::int AS c FROM questions")).rows[0].c;
  return s === 0 && u === 0 && q === 0;
}

async function migrateFromJsonFile(pool, jsonPath) {
  const raw = fs.readFileSync(jsonPath, "utf8");
  const j = JSON.parse(raw);
  const base = emptyDbObject();
  const db = {
    ...base,
    meta: {
      nextUserId: Number(j.meta?.nextUserId) || 1,
      nextAttemptId: Number(j.meta?.nextAttemptId) || 1,
      initializedAt: j.meta?.initializedAt || base.meta.initializedAt,
      updatedAt: j.meta?.updatedAt || base.meta.updatedAt,
    },
    settings: {
      examDurationMinutes: Number(j.settings?.examDurationMinutes) || DEFAULT_SETTINGS.examDurationMinutes,
      passScore: Number(j.settings?.passScore) || DEFAULT_SETTINGS.passScore,
      maxAttempts: Number(j.settings?.maxAttempts) || DEFAULT_SETTINGS.maxAttempts,
    },
    stores: Array.isArray(j.stores) ? j.stores : [],
    departments: Array.isArray(j.departments) ? j.departments : [],
    positions: Array.isArray(j.positions) ? j.positions : [],
    storeDepartmentMap: j.storeDepartmentMap && typeof j.storeDepartmentMap === "object" ? j.storeDepartmentMap : {},
    departmentPositionMap: j.departmentPositionMap && typeof j.departmentPositionMap === "object" ? j.departmentPositionMap : {},
    questions: Array.isArray(j.questions) ? j.questions : [],
    users: Array.isArray(j.users) ? j.users : [],
    attempts: Array.isArray(j.attempts) ? j.attempts : [],
    logs: Array.isArray(j.logs) ? j.logs : [],
  };
  await saveState(pool, db);
}

async function initPostgresStore(databaseUrl, legacyJsonPath) {
  const pool = createPool(databaseUrl);
  await createSchema(pool);

  const metaR = await pool.query("SELECT 1 FROM meta WHERE id = 1");
  if (!metaR.rows.length) {
    const t = nowIso();
    await pool.query(
      "INSERT INTO meta (id, next_user_id, next_attempt_id, initialized_at, updated_at) VALUES (1, 1, 1, $1, $2)",
      [t, t]
    );
    await pool.query(
      "INSERT INTO settings (id, exam_duration_minutes, pass_score, max_attempts) VALUES (1, $1, $2, $3)",
      [DEFAULT_SETTINGS.examDurationMinutes, DEFAULT_SETTINGS.passScore, DEFAULT_SETTINGS.maxAttempts]
    );
  }

  if (await shouldMigrateFromJson(pool, legacyJsonPath)) {
    await migrateFromJsonFile(pool, legacyJsonPath);
  }

  const db = (await loadState(pool)) || emptyDbObject();

  let saveChain = Promise.resolve();
  const store = {
    pool,
    db,
    save() {
      saveChain = saveChain
        .then(() => saveState(pool, db))
        .catch((err) => console.error("PostgreSQL 保存失败:", err));
    },
  };

  return store;
}

module.exports = {
  initPostgresStore,
  createPool,
  createSchema,
  loadState,
  saveState,
  emptyDbObject,
};
