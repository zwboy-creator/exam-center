/**
 * SQLite 持久化（Node 内置 node:sqlite，无需原生编译）
 * 与原先 app-data.json 相同的内存结构 load/save
 */
const fs = require("fs");
const path = require("path");
const { DatabaseSync } = require("node:sqlite");

const DEFAULT_SETTINGS = { examDurationMinutes: 60, passScore: 80, maxAttempts: 2 };

function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

function openDatabase(sqlitePath) {
  ensureDir(path.dirname(sqlitePath));
  const db = new DatabaseSync(sqlitePath);
  db.exec("PRAGMA journal_mode = WAL;");
  db.exec("PRAGMA foreign_keys = ON;");
  return db;
}

function createSchema(sql) {
  sql.exec(`
    CREATE TABLE IF NOT EXISTS meta (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      next_user_id INTEGER NOT NULL DEFAULT 1,
      next_attempt_id INTEGER NOT NULL DEFAULT 1,
      initialized_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS settings (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      exam_duration_minutes INTEGER NOT NULL DEFAULT 60,
      pass_score INTEGER NOT NULL DEFAULT 80,
      max_attempts INTEGER NOT NULL DEFAULT 2
    );
    CREATE TABLE IF NOT EXISTS stores (id TEXT PRIMARY KEY, name TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS departments (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS positions (id INTEGER PRIMARY KEY, name TEXT NOT NULL, category TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS store_department (store_id TEXT NOT NULL, department_id INTEGER NOT NULL, PRIMARY KEY (store_id, department_id));
    CREATE TABLE IF NOT EXISTS department_position (department_id INTEGER NOT NULL, position_id INTEGER NOT NULL, PRIMARY KEY (department_id, position_id));
    CREATE TABLE IF NOT EXISTS questions (
      id INTEGER PRIMARY KEY,
      category TEXT NOT NULL,
      question_type TEXT NOT NULL,
      content TEXT NOT NULL,
      options_json TEXT NOT NULL,
      answer TEXT NOT NULL,
      score REAL NOT NULL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS users (
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
    );
    CREATE TABLE IF NOT EXISTS attempts (
      id INTEGER PRIMARY KEY,
      user_id INTEGER NOT NULL,
      attempt_no INTEGER NOT NULL,
      is_makeup INTEGER NOT NULL DEFAULT 0,
      status TEXT NOT NULL,
      started_at TEXT,
      submitted_at TEXT,
      duration_minutes INTEGER NOT NULL,
      pass_score INTEGER NOT NULL,
      total_score REAL NOT NULL DEFAULT 0,
      raw_score REAL NOT NULL DEFAULT 0,
      pass INTEGER NOT NULL DEFAULT 0,
      submitted_reason TEXT NOT NULL DEFAULT '',
      anti_cheat_json TEXT NOT NULL DEFAULT '{}',
      marked_question_ids_json TEXT NOT NULL DEFAULT '[]',
      questions_json TEXT NOT NULL DEFAULT '[]'
    );
    CREATE TABLE IF NOT EXISTS logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      at TEXT NOT NULL,
      actor_id INTEGER,
      action TEXT NOT NULL,
      detail_json TEXT NOT NULL DEFAULT '{}'
    );
    CREATE INDEX IF NOT EXISTS idx_attempts_user ON attempts(user_id);
    CREATE INDEX IF NOT EXISTS idx_users_phone ON users(phone);
  `);
}

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

/** 从磁盘加载为与 JSON 版一致的 db 对象 */
function loadState(sql) {
  const metaRow = sql.prepare("SELECT * FROM meta WHERE id = 1").get();
  if (!metaRow) return null;

  const settingsRow = sql.prepare("SELECT * FROM settings WHERE id = 1").get();
  const settings = settingsRow
    ? {
        examDurationMinutes: settingsRow.exam_duration_minutes,
        passScore: settingsRow.pass_score,
        maxAttempts: settingsRow.max_attempts,
      }
    : { ...DEFAULT_SETTINGS };

  const stores = sql.prepare("SELECT id, name FROM stores ORDER BY id").all();
  const departments = sql.prepare("SELECT id, name FROM departments ORDER BY id").all();
  const positions = sql.prepare("SELECT id, name, category FROM positions ORDER BY id").all();

  const storeDepartmentMap = {};
  for (const row of sql.prepare("SELECT store_id, department_id FROM store_department").all()) {
    const k = String(row.store_id);
    if (!storeDepartmentMap[k]) storeDepartmentMap[k] = [];
    storeDepartmentMap[k].push(Number(row.department_id));
  }

  const departmentPositionMap = {};
  for (const row of sql.prepare("SELECT department_id, position_id FROM department_position").all()) {
    const k = String(row.department_id);
    if (!departmentPositionMap[k]) departmentPositionMap[k] = [];
    departmentPositionMap[k].push(Number(row.position_id));
  }

  const questions = sql
    .prepare("SELECT id, category, question_type AS questionType, content, options_json AS optionsJson, answer, score FROM questions ORDER BY id")
    .all()
    .map((q) => ({
      id: q.id,
      category: q.category,
      questionType: q.questionType,
      content: q.content,
      options: JSON.parse(q.optionsJson || "[]"),
      answer: q.answer,
      score: q.score,
    }));

  const users = sql
    .prepare(
      `SELECT id, name, phone, password_hash AS passwordHash, store_id AS storeId, department_id AS departmentId,
       position_id AS positionId, role, status, login_failures AS loginFailures, locked_until AS lockedUntil,
       created_at AS createdAt, updated_at AS updatedAt FROM users ORDER BY id`
    )
    .all();

  const attemptRows = sql.prepare("SELECT * FROM attempts ORDER BY id").all();
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

  const logs = sql
    .prepare("SELECT at, actor_id AS actorId, action, detail_json AS detailJson FROM logs ORDER BY id")
    .all()
    .map((r) => ({
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

/** 整库写入（与原先每次 save 覆盖 JSON 语义一致） */
function saveState(sql, db) {
  const t = nowIso();
  db.meta.updatedAt = t;

  const delLogs = sql.prepare("DELETE FROM logs");
  const delAttempts = sql.prepare("DELETE FROM attempts");
  const delUsers = sql.prepare("DELETE FROM users");
  const delQuestions = sql.prepare("DELETE FROM questions");
  const delDp = sql.prepare("DELETE FROM department_position");
  const delSd = sql.prepare("DELETE FROM store_department");
  const delPos = sql.prepare("DELETE FROM positions");
  const delDep = sql.prepare("DELETE FROM departments");
  const delStores = sql.prepare("DELETE FROM stores");

  const insMeta = sql.prepare(
    `INSERT INTO meta (id, next_user_id, next_attempt_id, initialized_at, updated_at) VALUES (1, ?, ?, ?, ?)
     ON CONFLICT(id) DO UPDATE SET next_user_id=excluded.next_user_id, next_attempt_id=excluded.next_attempt_id, updated_at=excluded.updated_at`
  );
  const insSettings = sql.prepare(
    `INSERT INTO settings (id, exam_duration_minutes, pass_score, max_attempts) VALUES (1, ?, ?, ?)
     ON CONFLICT(id) DO UPDATE SET exam_duration_minutes=excluded.exam_duration_minutes, pass_score=excluded.pass_score, max_attempts=excluded.max_attempts`
  );
  const insStore = sql.prepare("INSERT INTO stores (id, name) VALUES (?, ?)");
  const insDep = sql.prepare("INSERT INTO departments (id, name) VALUES (?, ?)");
  const insPos = sql.prepare("INSERT INTO positions (id, name, category) VALUES (?, ?, ?)");
  const insSd = sql.prepare("INSERT INTO store_department (store_id, department_id) VALUES (?, ?)");
  const insDp = sql.prepare("INSERT INTO department_position (department_id, position_id) VALUES (?, ?)");
  const insQ = sql.prepare(
    "INSERT INTO questions (id, category, question_type, content, options_json, answer, score) VALUES (?, ?, ?, ?, ?, ?, ?)"
  );
  const insUser = sql.prepare(
    `INSERT INTO users (id, name, phone, password_hash, store_id, department_id, position_id, role, status, login_failures, locked_until, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  );
  const insAttempt = sql.prepare(
    `INSERT INTO attempts (id, user_id, attempt_no, is_makeup, status, started_at, submitted_at, duration_minutes, pass_score, total_score, raw_score, pass, submitted_reason, anti_cheat_json, marked_question_ids_json, questions_json)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  );
  const insLog = sql.prepare("INSERT INTO logs (at, actor_id, action, detail_json) VALUES (?, ?, ?, ?)");

  sql.exec("BEGIN IMMEDIATE");
  try {
    delLogs.run();
    delAttempts.run();
    delUsers.run();
    delQuestions.run();
    delDp.run();
    delSd.run();
    delPos.run();
    delDep.run();
    delStores.run();

    insMeta.run(
      db.meta.nextUserId,
      db.meta.nextAttemptId,
      db.meta.initializedAt || t,
      db.meta.updatedAt
    );
    insSettings.run(db.settings.examDurationMinutes, db.settings.passScore, db.settings.maxAttempts);

    for (const s of db.stores) insStore.run(String(s.id), String(s.name));
    for (const d of db.departments) insDep.run(Number(d.id), String(d.name));
    for (const p of db.positions) insPos.run(Number(p.id), String(p.name), String(p.category));

    const sdm = db.storeDepartmentMap || {};
    for (const storeId of Object.keys(sdm)) {
      for (const depId of sdm[storeId] || []) {
        insSd.run(String(storeId), Number(depId));
      }
    }

    const dpm = db.departmentPositionMap || {};
    for (const depId of Object.keys(dpm)) {
      for (const posId of dpm[depId] || []) {
        insDp.run(Number(depId), Number(posId));
      }
    }

    for (const q of db.questions) {
      insQ.run(
        q.id,
        q.category,
        q.questionType,
        q.content,
        JSON.stringify(q.options || []),
        q.answer,
        Number(q.score) || 0
      );
    }

    for (const u of db.users) {
      insUser.run(
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
        u.updatedAt
      );
    }

    for (const a of db.attempts) {
      insAttempt.run(
        a.id,
        a.userId,
        a.attemptNo,
        a.isMakeup ? 1 : 0,
        a.status,
        a.startedAt,
        a.submittedAt,
        a.durationMinutes,
        a.passScore,
        a.totalScore,
        a.rawScore,
        a.pass ? 1 : 0,
        a.submittedReason || "",
        JSON.stringify(a.antiCheat || {}),
        JSON.stringify(a.markedQuestionIds || []),
        JSON.stringify(a.questions || [])
      );
    }

    for (const L of db.logs || []) {
      insLog.run(L.at, L.actorId ?? null, L.action, JSON.stringify(L.detail || {}));
    }

    sql.exec("COMMIT");
  } catch (e) {
    try {
      sql.exec("ROLLBACK");
    } catch (_) {
      /* 忽略回滚失败 */
    }
    throw e;
  }
}

/**
 * 若存在旧版 app-data.json 且当前库无任何业务数据，则导入一次
 */
function shouldMigrateFromJson(sql, jsonPath) {
  if (!fs.existsSync(jsonPath)) return false;
  const nStores = sql.prepare("SELECT COUNT(*) AS c FROM stores").get().c;
  const nUsers = sql.prepare("SELECT COUNT(*) AS c FROM users").get().c;
  const nQuestions = sql.prepare("SELECT COUNT(*) AS c FROM questions").get().c;
  return nStores === 0 && nUsers === 0 && nQuestions === 0;
}

function migrateFromJsonFile(sql, jsonPath) {
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
  saveState(sql, db);
}

function initSqliteStore(sqlitePath, legacyJsonPath) {
  const sql = openDatabase(sqlitePath);
  createSchema(sql);

  const metaRow = sql.prepare("SELECT 1 FROM meta WHERE id = 1").get();
  if (!metaRow) {
    const t = nowIso();
    sql
      .prepare("INSERT INTO meta (id, next_user_id, next_attempt_id, initialized_at, updated_at) VALUES (1, 1, 1, ?, ?)")
      .run(t, t);
    sql
      .prepare(
        "INSERT INTO settings (id, exam_duration_minutes, pass_score, max_attempts) VALUES (1, ?, ?, ?)"
      )
      .run(DEFAULT_SETTINGS.examDurationMinutes, DEFAULT_SETTINGS.passScore, DEFAULT_SETTINGS.maxAttempts);
  }

  if (shouldMigrateFromJson(sql, legacyJsonPath)) {
    migrateFromJsonFile(sql, legacyJsonPath);
  }

  const db = loadState(sql) || emptyDbObject();

  return {
    sql,
    db,
    save() {
      saveState(sql, db);
    },
  };
}

module.exports = {
  initSqliteStore,
  loadState,
  saveState,
  emptyDbObject,
};
