require("dotenv").config();
const fs = require("fs");
const path = require("path");
const express = require("express");
const session = require("express-session");
const bcrypt = require("bcryptjs");
const multer = require("multer");
const { parse } = require("csv-parse/sync");
const iconv = require("iconv-lite");
const XLSX = require("xlsx");
const { initSqliteStore } = require("./db/sqlite-store");
const { initPostgresStore } = require("./db/postgres-store");

const app = express();
if (process.env.TRUST_PROXY === "1") {
  app.set("trust proxy", 1);
}
const upload = multer({ storage: multer.memoryStorage() });

const PORT = Number(process.env.PORT || 8080);
const PHONE_RE = /^1\d{10}$/;
const ROLES = ["employee", "manager", "admin"];
const STATUS = ["active", "disabled"];
const CATS = ["management", "service", "technical", "spare_parts", "support", "universal"];
const ROLE_LABEL = { employee: "员工", manager: "店长/主管", admin: "系统管理员" };
const CAT_LABEL = {
  management: "管理岗",
  service: "服务岗",
  technical: "技术岗",
  spare_parts: "配件/索赔岗",
  support: "职能岗",
  universal: "通用岗",
};

const SQLITE_PATH = process.env.SQLITE_PATH || path.join(__dirname, "data", "app.db");
const LEGACY_JSON_PATH = path.join(__dirname, "data", "app-data.json");
const SEED_DIR = path.join(__dirname, "..", "seed");

function now() { return new Date().toISOString(); }

let store;
let db;
function save() {
  if (!db) return;
  db.meta.updatedAt = now();
  store.save();
}

async function bootstrapDatabase() {
  const url = String(process.env.DATABASE_URL || "").trim();
  if (url) {
    const maxTry = 6;
    let lastErr;
    for (let i = 0; i < maxTry; i += 1) {
      try {
        store = await initPostgresStore(url, LEGACY_JSON_PATH);
        db = store.db;
        return;
      } catch (e) {
        lastErr = e;
        console.error(`PostgreSQL 连接失败 (${i + 1}/${maxTry}):`, e.message || e);
        await new Promise((r) => setTimeout(r, 4000));
      }
    }
    throw lastErr;
  }
  store = initSqliteStore(SQLITE_PATH, LEGACY_JSON_PATH);
  db = store.db;
}

function csvRows(file) {
  const p = path.join(SEED_DIR, file);
  if (!fs.existsSync(p)) return [];
  const buf = fs.readFileSync(p);
  const text = iconv.decode(buf, "gb18030");
  return parse(text, { columns: true, skip_empty_lines: true, trim: true });
}

function parseArray(v) {
  if (!v) return [];
  try { const a = JSON.parse(v); return Array.isArray(a) ? a : []; } catch { return []; }
}

function toNum(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function toPositiveInt(v) {
  const n = Number(v);
  return Number.isInteger(n) && n > 0 ? n : null;
}

function dtText(v) {
  if (!v) return "";
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return String(v);
  return d.toLocaleString("zh-CN", { hour12: false });
}

function normalizeNumArray(value, validSet) {
  const arr = Array.isArray(value) ? value : (value ? [value] : []);
  const out = [];
  arr.forEach((item) => {
    const n = toPositiveInt(item);
    if (n && (!validSet || validSet.has(n)) && !out.includes(n)) out.push(n);
  });
  return out;
}

function ensureAssociationMaps() {
  if (!db.storeDepartmentMap || typeof db.storeDepartmentMap !== "object") {
    db.storeDepartmentMap = {};
  }
  if (!db.departmentPositionMap || typeof db.departmentPositionMap !== "object") {
    db.departmentPositionMap = {};
  }

  const depIds = db.departments.map((d) => Number(d.id));
  const posIds = db.positions.map((p) => Number(p.id));
  const depSet = new Set(depIds);
  const posSet = new Set(posIds);

  db.stores.forEach((s) => {
    const key = String(s.id);
    const existing = normalizeNumArray(db.storeDepartmentMap[key], depSet);
    db.storeDepartmentMap[key] = existing.length ? existing : [...depIds];
  });

  db.departments.forEach((d) => {
    const key = String(d.id);
    const existing = normalizeNumArray(db.departmentPositionMap[key], posSet);
    db.departmentPositionMap[key] = existing.length ? existing : [...posIds];
  });
}

function getDepartmentIdsForStore(storeId) {
  const key = String(storeId || "");
  const depIds = db.departments.map((d) => Number(d.id));
  const depSet = new Set(depIds);
  const mapped = normalizeNumArray((db.storeDepartmentMap || {})[key], depSet);
  return mapped.length ? mapped : depIds;
}

function getPositionIdsForDepartment(departmentId) {
  const key = String(departmentId || "");
  const posIds = db.positions.map((p) => Number(p.id));
  const posSet = new Set(posIds);
  const mapped = normalizeNumArray((db.departmentPositionMap || {})[key], posSet);
  return mapped.length ? mapped : posIds;
}

function isLinkedStoreDepartment(storeId, departmentId) {
  return getDepartmentIdsForStore(storeId).includes(Number(departmentId));
}

function isLinkedDepartmentPosition(departmentId, positionId) {
  return getPositionIdsForDepartment(departmentId).includes(Number(positionId));
}

function validateStoreDepartmentPosition(storeId, departmentId, positionId) {
  if (!findStore(storeId) || !findDep(departmentId) || !findPos(positionId)) {
    return { ok: false, message: "门店/部门/岗位必须选择系统配置项" };
  }
  if (!isLinkedStoreDepartment(storeId, departmentId)) {
    return { ok: false, message: "该门店未关联所选部门，请重新选择" };
  }
  if (!isLinkedDepartmentPosition(departmentId, positionId)) {
    return { ok: false, message: "该部门未关联所选岗位，请重新选择" };
  }
  return { ok: true };
}

function relationViewData() {
  return {
    storeDepartmentMap: db.storeDepartmentMap || {},
    departmentPositionMap: db.departmentPositionMap || {},
  };
}

function findStore(id) { return db.stores.find((x) => x.id === String(id)); }
function findDep(id) { return db.departments.find((x) => x.id === Number(id)); }
function findPos(id) { return db.positions.find((x) => x.id === Number(id)); }
function storeName(id) { const x = findStore(id); return x ? x.name : "-"; }
function depName(id) { const x = findDep(id); return x ? x.name : "-"; }
function posName(id) { const x = findPos(id); return x ? x.name : "-"; }
function posCat(id) { const x = findPos(id); return x ? x.category : "universal"; }
function hasAdmin() { return db.users.some((u) => u.role === "admin" && u.status === "active"); }
function flash(req, msg, type = "info") { req.session.flash = { msg, type }; }
function norm(ans) {
  return String(ans || "").split(",").map((s) => s.trim().toUpperCase()).filter(Boolean).sort().join(",");
}
function pick(record, keys) {
  for (const k of keys) {
    if (Object.prototype.hasOwnProperty.call(record, k)) {
      const v = String(record[k] || "").trim();
      if (v) return v;
    }
  }
  return "";
}
function log(actorId, action, detail) { db.logs.push({ at: now(), actorId, action, detail }); }
function shuffle(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}
function pickQuestions(chosen, category, questionType, count) {
  const pool = db.questions.filter((q) => q.category === category && q.questionType === questionType && !chosen.has(q.id));
  if (pool.length < count) throw new Error(`题库不足：${category}/${questionType}`);
  const out = shuffle(pool).slice(0, count);
  out.forEach((q) => chosen.add(q.id));
  return out;
}
function buildPaper(user) {
  const chosen = new Set();
  const cat = posCat(user.positionId) === "universal" ? "support" : posCat(user.positionId);
  const qs = shuffle([
    ...pickQuestions(chosen, "universal", "single", 20),
    ...pickQuestions(chosen, "universal", "multiple", 10),
    ...pickQuestions(chosen, cat, "single", 10),
    ...pickQuestions(chosen, cat, "multiple", 5),
  ]);
  return qs.map((q, i) => ({
    questionId: q.id, order: i + 1, category: q.category, questionType: q.questionType,
    content: q.content, options: q.options, correctAnswer: q.answer, score: Number(q.score), selectedAnswer: "", isCorrect: false,
  }));
}
function enrich(u) {
  return {
    ...u,
    storeName: storeName(u.storeId), departmentName: depName(u.departmentId), positionName: posName(u.positionId),
    roleLabel: ROLE_LABEL[u.role] || u.role, categoryLabel: CAT_LABEL[posCat(u.positionId)] || posCat(u.positionId),
  };
}
function currentUser(req) {
  const id = req.session.userId;
  if (!id) return null;
  const user = db.users.find((x) => x.id === id);
  if (!user || user.status !== "active") { req.session.userId = null; return null; }
  return user;
}
function requireAuth(req, res, next) {
  const u = currentUser(req);
  if (!u) { flash(req, "请先登录。", "warning"); return res.redirect("/login"); }
  req.currentUser = u;
  return next();
}
function requireRole(roles) {
  return (req, res, next) => {
    if (!req.currentUser || !roles.includes(req.currentUser.role)) {
      flash(req, "无权限访问。", "danger");
      return res.redirect("/dashboard");
    }
    return next();
  };
}
function userAttempts(uid) {
  return db.attempts.filter((a) => a.userId === uid).sort((a, b) => new Date(b.startedAt) - new Date(a.startedAt));
}
function startAttempt(user) {
  const list = userAttempts(user.id);
  if (list.some((x) => x.status === "in_progress")) throw new Error("已有进行中的考试");
  if (list.some((x) => x.pass)) throw new Error("已通过考试，无需再考");
  const submitted = list.filter((x) => x.status === "submitted").length;
  if (submitted >= db.settings.maxAttempts) throw new Error("已达到最大考试次数");
  const item = {
    id: db.meta.nextAttemptId++, userId: user.id, attemptNo: submitted + 1, isMakeup: submitted > 0,
    status: "in_progress", startedAt: now(), submittedAt: null,
    durationMinutes: Number(db.settings.examDurationMinutes) || 60, passScore: Number(db.settings.passScore) || 80,
    totalScore: 0, rawScore: 0, pass: false, submittedReason: "",
    antiCheat: { isCheated: false, violationCount: 0, events: [] },
    markedQuestionIds: [],
    questions: buildPaper(user),
  };
  db.attempts.push(item);
  return item;
}
function submitAttempt(attempt, body, options = {}) {
  if (attempt.status !== "in_progress") return;
  const payload = body || {};
  const reason = options.reason || "manual";
  const forceCheatSubmit = reason === "cheat" || String(payload.forceCheatSubmit || "") === "1";
  const incomingViolationCount = toNum(payload.cheatViolationCount, 0);
  let incomingEvents = [];
  if (payload.cheatEventsJson) {
    try {
      const parsed = JSON.parse(payload.cheatEventsJson);
      if (Array.isArray(parsed)) incomingEvents = parsed;
    } catch { /* ignore invalid */ }
  }

  let total = 0;
  attempt.questions = attempt.questions.map((q) => {
    const v = payload[`q_${q.questionId}`];
    const selected = Array.isArray(v) ? v.join(",") : (typeof v === "string" ? v : "");
    const ok = norm(selected) && norm(selected) === norm(q.correctAnswer);
    if (ok) total += Number(q.score);
    return { ...q, selectedAnswer: norm(selected), isCorrect: ok };
  });

  const oldAntiCheat = attempt.antiCheat || { violationCount: 0, events: [], isCheated: false };
  const violationCount = Math.max(toNum(oldAntiCheat.violationCount, 0), incomingViolationCount);
  const mergedEvents = [...(Array.isArray(oldAntiCheat.events) ? oldAntiCheat.events : []), ...incomingEvents]
    .filter((x) => x && typeof x === "object")
    .slice(-200);
  const isCheated = forceCheatSubmit || violationCount >= 3 || oldAntiCheat.isCheated;

  attempt.rawScore = total;
  attempt.totalScore = isCheated ? 0 : total;
  attempt.pass = !isCheated && total >= Number(attempt.passScore || 80);
  attempt.submittedReason = isCheated ? "cheat" : reason;
  attempt.antiCheat = {
    isCheated,
    violationCount,
    events: mergedEvents,
  };
  attempt.status = "submitted";
  attempt.submittedAt = now();
}

app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));
app.use(express.static(path.join(__dirname, "public")));
// 探活放在 session 之前，避免依赖会话与业务中间件（Render / 浏览器均可访问）
function healthHandler(req, res) {
  res.status(200).type("text/plain").send("ok");
}
app.get("/health", healthHandler);
app.head("/health", healthHandler);

app.use(express.urlencoded({ extended: true }));
app.use(express.json({ limit: "1mb" }));
app.use(session({ secret: process.env.SESSION_SECRET || "employee-handbook-exam-secret", resave: false, saveUninitialized: false }));
app.use((req, res, next) => {
  const u = currentUser(req);
  if (u) req.currentUser = u;
  res.locals.currentUser = u ? enrich(u) : null;
  res.locals.flash = req.session.flash || null;
  delete req.session.flash;
  res.locals.year = new Date().getFullYear();
  next();
});

app.get("/", (req, res) => {
  if (!req.currentUser) return res.redirect("/login");
  if (req.currentUser.role === "admin") return res.redirect("/admin/users");
  return res.redirect("/dashboard");
});

app.get("/setup", (req, res) => {
  if (hasAdmin()) {
    flash(req, "系统已存在启用中的管理员，请直接登录；如需新增管理员，请在后台用户管理中创建。", "warning");
    return res.redirect("/login");
  }
  return res.render("setup", {
    stores: db.stores,
    departments: db.departments,
    positions: db.positions,
    relations: relationViewData(),
  });
});

app.post("/setup", async (req, res) => {
  if (hasAdmin()) {
    flash(req, "系统已存在启用中的管理员，不能重复初始化。", "warning");
    return res.redirect("/login");
  }
  const { name, phone, password, passwordConfirm, storeId, departmentId, positionId } = req.body;
  if (!name || !phone || !password || !passwordConfirm) { flash(req, "请完整填写管理员信息", "danger"); return res.redirect("/setup"); }
  if (!PHONE_RE.test(phone)) { flash(req, "手机号格式错误", "danger"); return res.redirect("/setup"); }
  if (password !== passwordConfirm) { flash(req, "两次密码不一致", "danger"); return res.redirect("/setup"); }
  const validChain = validateStoreDepartmentPosition(storeId, departmentId, positionId);
  if (!validChain.ok) { flash(req, validChain.message, "danger"); return res.redirect("/setup"); }
  if (db.users.some((u) => u.phone === phone)) { flash(req, "手机号已被使用", "danger"); return res.redirect("/setup"); }
  db.users.push({
    id: db.meta.nextUserId++, name: String(name).trim(), phone: String(phone).trim(), passwordHash: await bcrypt.hash(password, 10),
    storeId: String(storeId), departmentId: Number(departmentId), positionId: Number(positionId),
    role: "admin", status: "active", loginFailures: 0, lockedUntil: null, createdAt: now(), updatedAt: now(),
  });
  save();
  flash(req, "管理员已创建，请登录", "success");
  return res.redirect("/login");
});

app.get("/register", (req, res) => {
  return res.render("register", {
    stores: db.stores,
    departments: db.departments,
    positions: db.positions,
    relations: relationViewData(),
  });
});

app.post("/register", async (req, res) => {
  const { name, phone, password, passwordConfirm, storeId, departmentId, positionId } = req.body;
  if (!name || !phone || !password || !passwordConfirm) { flash(req, "请完整填写注册信息", "danger"); return res.redirect("/register"); }
  if (!PHONE_RE.test(phone)) { flash(req, "手机号格式错误", "danger"); return res.redirect("/register"); }
  if (password !== passwordConfirm) { flash(req, "两次密码不一致", "danger"); return res.redirect("/register"); }
  const validChain = validateStoreDepartmentPosition(storeId, departmentId, positionId);
  if (!validChain.ok) { flash(req, validChain.message, "danger"); return res.redirect("/register"); }
  if (db.users.some((u) => u.phone === phone)) { flash(req, "手机号已注册", "warning"); return res.redirect("/login"); }
  db.users.push({
    id: db.meta.nextUserId++, name: String(name).trim(), phone: String(phone).trim(), passwordHash: await bcrypt.hash(password, 10),
    storeId: String(storeId), departmentId: Number(departmentId), positionId: Number(positionId),
    role: "employee", status: "active", loginFailures: 0, lockedUntil: null, createdAt: now(), updatedAt: now(),
  });
  save();
  flash(req, "注册成功，请登录", "success");
  return res.redirect("/login");
});

app.get("/login", (req, res) => res.render("login", { hasAdmin: hasAdmin() }));

app.post("/login", async (req, res) => {
  const phone = String(req.body.phone || "").trim();
  const password = String(req.body.password || "");
  const u = db.users.find((x) => x.phone === phone);
  if (!u) { flash(req, "手机号或密码错误", "danger"); return res.redirect("/login"); }
  if (u.status !== "active") { flash(req, "账号已禁用", "danger"); return res.redirect("/login"); }
  if (u.lockedUntil && Date.now() < new Date(u.lockedUntil).getTime()) { flash(req, "账号已临时锁定15分钟", "danger"); return res.redirect("/login"); }
  const ok = await bcrypt.compare(password, u.passwordHash);
  if (!ok) {
    u.loginFailures = Number(u.loginFailures || 0) + 1;
    if (u.loginFailures >= 5) { u.loginFailures = 0; u.lockedUntil = new Date(Date.now() + 15 * 60 * 1000).toISOString(); }
    u.updatedAt = now();
    save();
    flash(req, "手机号或密码错误", "danger");
    return res.redirect("/login");
  }
  u.loginFailures = 0;
  u.lockedUntil = null;
  u.updatedAt = now();
  req.session.userId = u.id;
  save();
  if (u.role === "admin") return res.redirect("/admin/users");
  return res.redirect("/dashboard");
});

app.post("/logout", (req, res) => req.session.destroy(() => res.redirect("/login")));

app.get("/dashboard", requireAuth, (req, res) => {
  if (req.currentUser.role === "admin") return res.redirect("/admin/users");
  const attempts = userAttempts(req.currentUser.id);
  const inProgress = attempts.find((x) => x.status === "in_progress") || null;
  const hasPassed = attempts.some((x) => x.pass);
  const submittedCount = attempts.filter((x) => x.status === "submitted").length;
  return res.render("dashboard", {
    user: enrich(req.currentUser), attempts, inProgress, hasPassed, submittedCount, maxAttempts: db.settings.maxAttempts, settings: db.settings,
  });
});

app.post("/exam/start", requireAuth, requireRole(["employee", "manager"]), (req, res) => {
  try {
    const a = startAttempt(req.currentUser);
    log(req.currentUser.id, "exam.start", { attemptId: a.id });
    save();
    return res.redirect(`/exam/${a.id}`);
  } catch (e) {
    flash(req, e.message || "创建考试失败", "danger");
    return res.redirect("/dashboard");
  }
});

app.get("/exam/:id", requireAuth, requireRole(["employee", "manager"]), (req, res) => {
  const a = db.attempts.find((x) => x.id === Number(req.params.id));
  if (!a || a.userId !== req.currentUser.id) { flash(req, "考试不存在", "warning"); return res.redirect("/dashboard"); }
  if (a.status === "submitted") return res.redirect(`/exam/${a.id}/result`);
  const deadline = new Date(new Date(a.startedAt).getTime() + a.durationMinutes * 60000);
  const remain = deadline.getTime() - Date.now();
  if (remain <= 0) {
    submitAttempt(a, {}, { reason: "timeout" });
    log(req.currentUser.id, "exam.auto_submit", { attemptId: a.id });
    save();
    flash(req, "已超时，系统自动交卷", "warning");
    return res.redirect(`/exam/${a.id}/result`);
  }
  return res.render("exam", { attempt: a, user: enrich(req.currentUser), deadlineIso: deadline.toISOString(), remainingSeconds: Math.floor(remain / 1000) });
});

app.post("/exam/:id/progress", requireAuth, requireRole(["employee", "manager"]), (req, res) => {
  const a = db.attempts.find((x) => x.id === Number(req.params.id));
  if (!a || a.userId !== req.currentUser.id) {
    return res.status(404).json({ ok: false, message: "考试不存在" });
  }
  if (a.status !== "in_progress") {
    return res.status(409).json({ ok: false, message: "考试已结束，无法保存进度" });
  }

  const payload = req.body || {};
  const answers = payload.answers && typeof payload.answers === "object" ? payload.answers : {};
  const rawMarked = Array.isArray(payload.markedQuestionIds) ? payload.markedQuestionIds : [];
  const markedSet = new Set(rawMarked.map((x) => String(x)));

  a.questions = a.questions.map((q) => {
    const raw = answers[String(q.questionId)] ?? answers[`q_${q.questionId}`] ?? "";
    const selected = Array.isArray(raw) ? raw.join(",") : String(raw || "");
    return { ...q, selectedAnswer: norm(selected) };
  });

  a.markedQuestionIds = a.questions
    .filter((q) => markedSet.has(String(q.questionId)))
    .map((q) => Number(q.questionId));

  save();
  return res.json({
    ok: true,
    savedAt: now(),
    markedCount: a.markedQuestionIds.length,
  });
});

app.post("/exam/:id/submit", requireAuth, requireRole(["employee", "manager"]), (req, res) => {
  const a = db.attempts.find((x) => x.id === Number(req.params.id));
  if (!a || a.userId !== req.currentUser.id) { flash(req, "考试不存在", "warning"); return res.redirect("/dashboard"); }
  const isCheat = String((req.body || {}).forceCheatSubmit || "") === "1";
  submitAttempt(a, req.body || {}, { reason: isCheat ? "cheat" : "manual" });
  log(req.currentUser.id, "exam.submit", {
    attemptId: a.id,
    score: a.totalScore,
    rawScore: a.rawScore,
    pass: a.pass,
    submittedReason: a.submittedReason,
    antiCheat: a.antiCheat,
  });
  save();
  return res.redirect(`/exam/${a.id}/result`);
});

app.get("/exam/:id/result", requireAuth, requireRole(["employee", "manager", "admin"]), (req, res) => {
  const a = db.attempts.find((x) => x.id === Number(req.params.id));
  if (!a) { flash(req, "考试不存在", "warning"); return res.redirect("/dashboard"); }
  if (req.currentUser.role !== "admin" && a.userId !== req.currentUser.id) { flash(req, "无权限查看", "danger"); return res.redirect("/dashboard"); }
  return res.render("result", { attempt: a, wrongCount: a.questions.filter((q) => !q.isCorrect).length, passScore: a.passScore });
});
app.get("/admin/users", requireAuth, requireRole(["admin"]), (req, res) => {
  const q = String(req.query.q || "").trim();
  const users = (q ? db.users.filter((u) => u.name.includes(q) || u.phone.includes(q)) : db.users)
    .map((u) => enrich(u))
    .sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  return res.render("admin-users", { users, keyword: q });
});

app.get("/admin/users/new", requireAuth, requireRole(["admin"]), (req, res) => {
  return res.render("admin-user-form", {
    mode: "create",
    target: null,
    stores: db.stores,
    departments: db.departments,
    positions: db.positions,
    roleOptions: ROLES,
    statusOptions: STATUS,
    relations: relationViewData(),
  });
});

app.post("/admin/users/new", requireAuth, requireRole(["admin"]), async (req, res) => {
  const { name, phone, password, storeId, departmentId, positionId, role, status } = req.body;
  if (!name || !phone || !password) { flash(req, "请完整填写必填项", "danger"); return res.redirect("/admin/users/new"); }
  if (!PHONE_RE.test(phone)) { flash(req, "手机号格式错误", "danger"); return res.redirect("/admin/users/new"); }
  if (db.users.some((u) => u.phone === phone)) { flash(req, "手机号已存在", "danger"); return res.redirect("/admin/users/new"); }
  const validChain = validateStoreDepartmentPosition(storeId, departmentId, positionId);
  if (!validChain.ok) { flash(req, validChain.message, "danger"); return res.redirect("/admin/users/new"); }
  db.users.push({
    id: db.meta.nextUserId++, name: String(name).trim(), phone: String(phone).trim(), passwordHash: await bcrypt.hash(password, 10),
    storeId: String(storeId), departmentId: Number(departmentId), positionId: Number(positionId), role: ROLES.includes(role) ? role : "employee", status: STATUS.includes(status) ? status : "active",
    loginFailures: 0, lockedUntil: null, createdAt: now(), updatedAt: now(),
  });
  save();
  flash(req, "用户创建成功", "success");
  return res.redirect("/admin/users");
});

app.get("/admin/users/:id/edit", requireAuth, requireRole(["admin"]), (req, res) => {
  const t = db.users.find((u) => u.id === Number(req.params.id));
  if (!t) { flash(req, "用户不存在", "warning"); return res.redirect("/admin/users"); }
  return res.render("admin-user-form", {
    mode: "edit",
    target: enrich(t),
    stores: db.stores,
    departments: db.departments,
    positions: db.positions,
    roleOptions: ROLES,
    statusOptions: STATUS,
    relations: relationViewData(),
  });
});

app.post("/admin/users/:id/edit", requireAuth, requireRole(["admin"]), async (req, res) => {
  const id = Number(req.params.id);
  const t = db.users.find((u) => u.id === id);
  if (!t) { flash(req, "用户不存在", "warning"); return res.redirect("/admin/users"); }
  const { name, phone, password, storeId, departmentId, positionId, role, status } = req.body;
  if (!name || !phone) { flash(req, "姓名和手机号不能为空", "danger"); return res.redirect(`/admin/users/${id}/edit`); }
  if (!PHONE_RE.test(phone)) { flash(req, "手机号格式错误", "danger"); return res.redirect(`/admin/users/${id}/edit`); }
  if (db.users.some((u) => u.phone === phone && u.id !== id)) { flash(req, "手机号已被使用", "danger"); return res.redirect(`/admin/users/${id}/edit`); }
  const validChain = validateStoreDepartmentPosition(storeId, departmentId, positionId);
  if (!validChain.ok) { flash(req, validChain.message, "danger"); return res.redirect(`/admin/users/${id}/edit`); }
  if (id === req.currentUser.id && status === "disabled") { flash(req, "不能禁用当前管理员", "danger"); return res.redirect(`/admin/users/${id}/edit`); }
  t.name = String(name).trim();
  t.phone = String(phone).trim();
  t.storeId = String(storeId); t.departmentId = Number(departmentId); t.positionId = Number(positionId);
  t.role = ROLES.includes(role) ? role : "employee"; t.status = STATUS.includes(status) ? status : "active";
  if (password && String(password).trim()) t.passwordHash = await bcrypt.hash(String(password).trim(), 10);
  t.updatedAt = now();
  save();
  flash(req, "用户更新成功", "success");
  return res.redirect("/admin/users");
});

app.get("/admin/users/import", requireAuth, requireRole(["admin"]), (req, res) => {
  const result = req.session.importResult || null;
  delete req.session.importResult;
  return res.render("admin-import-users", { result });
});

app.post("/admin/users/import", requireAuth, requireRole(["admin"]), upload.single("file"), async (req, res) => {
  if (!req.file) { flash(req, "请上传CSV", "danger"); return res.redirect("/admin/users/import"); }
  let rows = [];
  try {
    rows = parse(req.file.buffer.toString("utf8"), { columns: true, skip_empty_lines: true, trim: true });
  } catch (e) {
    flash(req, `CSV解析失败：${e.message}`, "danger");
    return res.redirect("/admin/users/import");
  }
  const result = { total: rows.length, created: 0, updated: 0, failed: 0, errors: [] };
  for (let i = 0; i < rows.length; i += 1) {
    const r = rows[i];
    const line = i + 2;
    const name = pick(r, ["name", "姓名", "real_name"]);
    const phone = pick(r, ["phone", "mobile", "手机号"]);
    const storeRaw = pick(r, ["store_id", "store", "store_name", "门店", "门店名称", "所在门店"]);
    const depRaw = pick(r, ["department_id", "department", "department_name", "部门", "部门名称", "所在部门"]);
    const posRaw = pick(r, ["position_id", "position", "position_name", "岗位", "岗位名称", "所在岗位"]);
    const roleRaw = pick(r, ["role", "角色"]);
    const statusRaw = pick(r, ["status", "状态"]);
    const pwdRaw = pick(r, ["password", "密码", "initial_password"]);
    const s = db.stores.find((x) => x.id === storeRaw || x.name === storeRaw);
    const d = db.departments.find((x) => String(x.id) === depRaw || x.name === depRaw);
    const p = db.positions.find((x) => String(x.id) === posRaw || x.name === posRaw);
    const rowErrors = [];
    if (!name) rowErrors.push("姓名为空");
    if (!phone) rowErrors.push("手机号为空");
    if (phone && !PHONE_RE.test(phone)) rowErrors.push(`手机号格式错误(${phone})`);
    if (!storeRaw) rowErrors.push("门店为空"); else if (!s) rowErrors.push(`门店"${storeRaw}"不存在`);
    if (!depRaw) rowErrors.push("部门为空"); else if (!d) rowErrors.push(`部门"${depRaw}"不存在`);
    if (!posRaw) rowErrors.push("岗位为空"); else if (!p) rowErrors.push(`岗位"${posRaw}"不存在`);
    if (rowErrors.length > 0) {
      result.failed += 1;
      if (result.errors.length < 20) {
        result.errors.push(`第${line}行导入失败：${rowErrors.join("；")}`);
      }
      continue;
    }
    const role = ROLES.includes(roleRaw) ? roleRaw : "employee";
    const status = STATUS.includes(statusRaw) ? statusRaw : "active";
    const pwd = pwdRaw || phone.slice(-6);
    const exists = db.users.find((x) => x.phone === phone);
    if (exists) {
      exists.name = name; exists.storeId = s.id; exists.departmentId = d.id; exists.positionId = p.id; exists.role = role; exists.status = status; exists.updatedAt = now();
      if (pwdRaw) exists.passwordHash = await bcrypt.hash(pwd, 10);
      result.updated += 1;
    } else {
      db.users.push({
        id: db.meta.nextUserId++, name, phone, passwordHash: await bcrypt.hash(pwd, 10),
        storeId: s.id, departmentId: d.id, positionId: p.id, role, status,
        loginFailures: 0, lockedUntil: null, createdAt: now(), updatedAt: now(),
      });
      result.created += 1;
    }
  }
  save();
  req.session.importResult = result;
  flash(req, `导入完成：新增${result.created}，更新${result.updated}，失败${result.failed}`, "success");
  return res.redirect("/admin/users/import");
});
app.get("/admin/config", requireAuth, requireRole(["admin"]), (req, res) => {
  return res.render("admin-config", {
    stores: db.stores,
    departments: db.departments,
    positions: db.positions,
    categories: CATS,
    relations: relationViewData(),
  });
});

app.post("/admin/config/store", requireAuth, requireRole(["admin"]), (req, res) => {
  const id = String(req.body.id || "").trim();
  const name = String(req.body.name || "").trim();
  if (!id || !name) { flash(req, "门店编码和名称不能为空", "danger"); return res.redirect("/admin/config"); }
  if (db.stores.some((x) => x.id === id || x.name === name)) { flash(req, "门店编码或名称已存在", "danger"); return res.redirect("/admin/config"); }
  db.stores.push({ id, name });
  db.storeDepartmentMap = db.storeDepartmentMap || {};
  db.storeDepartmentMap[String(id)] = db.departments.map((d) => Number(d.id));
  ensureAssociationMaps();
  save();
  flash(req, "门店新增成功", "success");
  return res.redirect("/admin/config");
});

app.post("/admin/config/department", requireAuth, requireRole(["admin"]), (req, res) => {
  const name = String(req.body.name || "").trim();
  const rawId = String(req.body.id || "").trim();
  const id = rawId ? Number(rawId) : Math.max(0, ...db.departments.map((x) => x.id)) + 1;
  if (!name || !Number.isInteger(id) || id <= 0) { flash(req, "部门名称或编码不合法", "danger"); return res.redirect("/admin/config"); }
  if (db.departments.some((x) => x.id === id || x.name === name)) { flash(req, "部门编码或名称已存在", "danger"); return res.redirect("/admin/config"); }
  db.departments.push({ id, name });
  db.storeDepartmentMap = db.storeDepartmentMap || {};
  db.stores.forEach((s) => {
    const key = String(s.id);
    const list = normalizeNumArray(db.storeDepartmentMap[key], null);
    if (!list.includes(id)) list.push(id);
    db.storeDepartmentMap[key] = list;
  });
  db.departmentPositionMap = db.departmentPositionMap || {};
  db.departmentPositionMap[String(id)] = db.positions.map((p) => Number(p.id));
  ensureAssociationMaps();
  save();
  flash(req, "部门新增成功", "success");
  return res.redirect("/admin/config");
});

app.post("/admin/config/position", requireAuth, requireRole(["admin"]), (req, res) => {
  const name = String(req.body.name || "").trim();
  const rawId = String(req.body.id || "").trim();
  const category = String(req.body.category || "").trim();
  const id = rawId ? Number(rawId) : Math.max(0, ...db.positions.map((x) => x.id)) + 1;
  if (!name || !CATS.includes(category) || !Number.isInteger(id) || id <= 0) { flash(req, "岗位参数不合法", "danger"); return res.redirect("/admin/config"); }
  if (db.positions.some((x) => x.id === id || x.name === name)) { flash(req, "岗位编码或名称已存在", "danger"); return res.redirect("/admin/config"); }
  db.positions.push({ id, name, category });
  db.departmentPositionMap = db.departmentPositionMap || {};
  db.departments.forEach((d) => {
    const key = String(d.id);
    const list = normalizeNumArray(db.departmentPositionMap[key], null);
    if (!list.includes(id)) list.push(id);
    db.departmentPositionMap[key] = list;
  });
  ensureAssociationMaps();
  save();
  flash(req, "岗位新增成功", "success");
  return res.redirect("/admin/config");
});

app.post("/admin/config/store-department-link", requireAuth, requireRole(["admin"]), (req, res) => {
  const storeId = String(req.body.storeId || "").trim();
  if (!findStore(storeId)) {
    flash(req, "门店不存在", "danger");
    return res.redirect("/admin/config");
  }

  const depSet = new Set(db.departments.map((d) => Number(d.id)));
  const departmentIds = normalizeNumArray(req.body.departmentIds, depSet);
  if (!departmentIds.length) {
    flash(req, "请至少关联一个部门", "danger");
    return res.redirect("/admin/config");
  }

  db.storeDepartmentMap = db.storeDepartmentMap || {};
  db.storeDepartmentMap[storeId] = departmentIds;
  ensureAssociationMaps();
  save();
  flash(req, "门店-部门关联已更新", "success");
  return res.redirect("/admin/config");
});

app.post("/admin/config/department-position-link", requireAuth, requireRole(["admin"]), (req, res) => {
  const departmentId = toPositiveInt(req.body.departmentId);
  if (!departmentId || !findDep(departmentId)) {
    flash(req, "部门不存在", "danger");
    return res.redirect("/admin/config");
  }

  const posSet = new Set(db.positions.map((p) => Number(p.id)));
  const positionIds = normalizeNumArray(req.body.positionIds, posSet);
  if (!positionIds.length) {
    flash(req, "请至少关联一个岗位", "danger");
    return res.redirect("/admin/config");
  }

  db.departmentPositionMap = db.departmentPositionMap || {};
  db.departmentPositionMap[String(departmentId)] = positionIds;
  ensureAssociationMaps();
  save();
  flash(req, "部门-岗位关联已更新", "success");
  return res.redirect("/admin/config");
});

app.get("/admin/questions", requireAuth, requireRole(["admin"]), (req, res) => {
  const map = new Map();
  for (const q of db.questions) {
    const k = `${q.category}__${q.questionType}`;
    map.set(k, (map.get(k) || 0) + 1);
  }
  const summary = Array.from(map.entries()).map(([k, count]) => {
    const [category, questionType] = k.split("__");
    return { category, questionType, count };
  });
  const latestQuestions = [...db.questions].sort((a, b) => b.id - a.id).slice(0, 30);
  return res.render("admin-questions", { total: db.questions.length, summary, latestQuestions });
});

app.post("/admin/questions/import", requireAuth, requireRole(["admin"]), upload.single("file"), (req, res) => {
  if (!req.file) { flash(req, "请上传题库CSV", "danger"); return res.redirect("/admin/questions"); }
  let rows = [];
  try {
    rows = parse(req.file.buffer.toString("utf8"), { columns: true, skip_empty_lines: true, trim: true });
  } catch (e) {
    flash(req, `题库CSV解析失败：${e.message}`, "danger");
    return res.redirect("/admin/questions");
  }
  const parsed = rows.map((r) => ({
    id: Number(r.id), category: String(r.category || "").trim(), questionType: String(r.question_type || r.questionType || "").trim(),
    content: String(r.content || "").trim(), options: parseArray(r.options), answer: String(r.answer || "").trim(), score: Number(r.score) || 0,
  })).filter((q) => Number.isInteger(q.id) && ["single", "multiple"].includes(q.questionType) && q.content);
  if (!parsed.length) { flash(req, "没有解析到有效题目", "danger"); return res.redirect("/admin/questions"); }
  const mode = String(req.body.mode || "replace");
  if (mode === "replace") {
    db.questions = parsed;
  } else {
    const m = new Map(db.questions.map((q) => [q.id, q]));
    parsed.forEach((q) => m.set(q.id, q));
    db.questions = Array.from(m.values()).sort((a, b) => a.id - b.id);
  }
  save();
  flash(req, `题库导入完成，共${parsed.length}题`, "success");
  return res.redirect("/admin/questions");
});

function listStoresForViewer(viewer) {
  if (viewer.role === "manager") {
    return db.stores.filter((s) => s.id === viewer.storeId);
  }
  return db.stores;
}

function getReportFilters(req, viewer) {
  const storesForViewer = listStoresForViewer(viewer);
  const storeIds = new Set(storesForViewer.map((s) => s.id));
  const positionIds = new Set(db.positions.map((p) => p.id));

  const ui = {
    storeId: String(req.query.storeId || "all"),
    positionId: String(req.query.positionId || "all"),
    batch: String(req.query.batch || "all"),
  };

  if (viewer.role === "manager") {
    ui.storeId = viewer.storeId;
  }

  const filterStoreId = ui.storeId === "all" ? null : ui.storeId;
  const filterPositionIdRaw = ui.positionId === "all" ? null : toPositiveInt(ui.positionId);
  const filterBatchNoRaw = ui.batch === "all" ? null : toPositiveInt(ui.batch);

  const filterStoreIdSafe = filterStoreId && storeIds.has(filterStoreId) ? filterStoreId : null;
  const filterPositionIdSafe = filterPositionIdRaw && positionIds.has(filterPositionIdRaw) ? filterPositionIdRaw : null;
  const filterBatchNoSafe = filterBatchNoRaw && filterBatchNoRaw > 0 ? filterBatchNoRaw : null;

  if (viewer.role !== "manager" && !filterStoreIdSafe) ui.storeId = "all";
  if (!filterPositionIdSafe) ui.positionId = "all";
  if (!filterBatchNoSafe) ui.batch = "all";

  return {
    ui,
    storeId: viewer.role === "manager" ? viewer.storeId : filterStoreIdSafe,
    positionId: filterPositionIdSafe,
    batchNo: filterBatchNoSafe,
  };
}

function getFilteredUsersForReport(viewer, filters) {
  return db.users.filter((u) => {
    if (u.role === "admin") return false;
    if (viewer.role === "manager" && u.storeId !== viewer.storeId) return false;
    if (filters.storeId && u.storeId !== filters.storeId) return false;
    if (filters.positionId && Number(u.positionId) !== Number(filters.positionId)) return false;
    return true;
  });
}

function getFilteredSubmittedAttempts(userIds, filters) {
  let submitted = db.attempts.filter((a) => a.status === "submitted" && userIds.has(a.userId));
  if (filters.batchNo) {
    submitted = submitted.filter((a) => Number(a.attemptNo) === Number(filters.batchNo));
  }
  return submitted;
}

function getLatestAttemptMap(attempts) {
  const map = new Map();
  attempts.forEach((a) => {
    const old = map.get(a.userId);
    if (!old || new Date(a.submittedAt) > new Date(old.submittedAt)) map.set(a.userId, a);
  });
  return map;
}

function buildStoreReportRows(viewer, filters) {
  const storesForViewer = listStoresForViewer(viewer);
  const stores = filters.storeId ? storesForViewer.filter((s) => s.id === filters.storeId) : storesForViewer;
  const allUsers = getFilteredUsersForReport(viewer, filters);

  return stores.map((s) => {
    const users = allUsers.filter((u) => u.storeId === s.id);
    const userIds = new Set(users.map((u) => u.id));
    const submitted = getFilteredSubmittedAttempts(userIds, filters);
    const participantIds = new Set(submitted.map((a) => a.userId));
    const passIds = new Set(submitted.filter((a) => a.pass).map((a) => a.userId));

    const latestMap = getLatestAttemptMap(submitted);
    const scores = Array.from(latestMap.values()).map((x) => Number(x.totalScore || 0));
    const avg = scores.length ? scores.reduce((sum, x) => sum + x, 0) / scores.length : 0;

    return {
      storeId: s.id,
      storeName: s.name,
      registeredCount: users.length,
      participantCount: participantIds.size,
      passCount: passIds.size,
      participationRate: users.length ? (participantIds.size / users.length) * 100 : 0,
      passRate: participantIds.size ? (passIds.size / participantIds.size) * 100 : 0,
      averageScore: avg,
    };
  });
}

function reasonLabel(reason) {
  if (reason === "timeout") return "超时自动提交";
  if (reason === "cheat") return "违规强制提交";
  return "手动提交";
}

function buildReportDetails(viewer, filters) {
  const users = getFilteredUsersForReport(viewer, filters);
  const userMap = new Map(users.map((u) => [u.id, u]));
  const userIds = new Set(users.map((u) => u.id));
  const submitted = getFilteredSubmittedAttempts(userIds, filters)
    .sort((a, b) => new Date(b.submittedAt) - new Date(a.submittedAt));

  return submitted.map((a, index) => {
    const u = userMap.get(a.userId);
    const antiCheat = a.antiCheat || { isCheated: false, violationCount: 0 };
    return {
      序号: index + 1,
      考试批次: a.attemptNo,
      考试类型: a.isMakeup ? "补考" : "初考",
      门店: storeName(u?.storeId),
      部门: depName(u?.departmentId),
      岗位: posName(u?.positionId),
      姓名: u?.name || "",
      手机号: u?.phone || "",
      开始时间: dtText(a.startedAt),
      提交时间: dtText(a.submittedAt),
      原始得分: Number(a.rawScore || a.totalScore || 0),
      最终得分: Number(a.totalScore || 0),
      合格状态: a.pass ? "合格" : "不合格",
      提交方式: reasonLabel(a.submittedReason),
      是否作弊: antiCheat.isCheated ? "是" : "否",
      违规次数: Number(antiCheat.violationCount || 0),
    };
  });
}

app.get("/reports/stores", requireAuth, requireRole(["manager", "admin"]), (req, res) => {
  const filters = getReportFilters(req, req.currentUser);
  const rows = buildStoreReportRows(req.currentUser, filters);
  const usersForRange = getFilteredUsersForReport(req.currentUser, filters);
  const batchOptions = Array.from(new Set(
    getFilteredSubmittedAttempts(new Set(usersForRange.map((u) => u.id)), { ...filters, batchNo: null }).map((a) => Number(a.attemptNo))
  )).sort((a, b) => a - b);

  return res.render("store-reports", {
    rows,
    isManager: req.currentUser.role === "manager",
    stores: listStoresForViewer(req.currentUser),
    positions: db.positions,
    batchOptions,
    filters: filters.ui,
  });
});

app.get("/reports/stores/export", requireAuth, requireRole(["manager", "admin"]), (req, res) => {
  const filters = getReportFilters(req, req.currentUser);
  const rows = buildStoreReportRows(req.currentUser, filters).map((x, idx) => ({
    序号: idx + 1,
    门店: x.storeName,
    注册人数: x.registeredCount,
    参与人数: x.participantCount,
    通过人数: x.passCount,
    参与率: `${x.participationRate.toFixed(2)}%`,
    通过率: `${x.passRate.toFixed(2)}%`,
    平均分: Number(x.averageScore.toFixed(2)),
  }));
  const detailRows = buildReportDetails(req.currentUser, filters);

  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(rows), "门店汇总");
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(detailRows), "成绩明细");

  const fileBuffer = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });
  const ts = new Date();
  const fileName = `门店学习考试统计_${ts.getFullYear()}${String(ts.getMonth() + 1).padStart(2, "0")}${String(ts.getDate()).padStart(2, "0")}.xlsx`;
  res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
  res.setHeader("Content-Disposition", `attachment; filename*=UTF-8''${encodeURIComponent(fileName)}`);
  return res.send(fileBuffer);
});

app.use((req, res) => res.status(404).render("not-found"));

async function seedIfEmpty() {
  if (!db.stores.length) {
    db.stores = csvRows("stores_rows.csv").map((r) => ({ id: String(r.id || "").trim(), name: String(r.name || "").trim() })).filter((x) => x.id && x.name);
  }
  if (!db.departments.length) {
    db.departments = csvRows("departments_rows.csv").map((r) => ({ id: Number(r.id), name: String(r.name || "").trim() })).filter((x) => Number.isInteger(x.id) && x.name);
  }
  if (!db.positions.length) {
    db.positions = csvRows("positions_rows.csv")
      .map((r) => ({ id: Number(r.id), name: String(r.name || "").trim(), category: String(r.category || "").trim() }))
      .filter((x) => Number.isInteger(x.id) && x.name && CATS.includes(x.category));
  }
  if (!db.questions.length) {
    db.questions = csvRows("questions_rows.csv")
      .map((r) => ({
        id: Number(r.id), category: String(r.category || "").trim(), questionType: String(r.question_type || "").trim(),
        content: String(r.content || "").trim(), options: parseArray(r.options), answer: String(r.answer || "").trim(), score: Number(r.score) || 0,
      }))
      .filter((q) => Number.isInteger(q.id) && ["single", "multiple"].includes(q.questionType) && q.content && q.options.length && q.answer);
  }
  ensureAssociationMaps();
  save();
}

bootstrapDatabase()
  .then(() => seedIfEmpty())
  .then(() => {
    app.listen(PORT, "0.0.0.0", () => {
      console.log(`员工手册学习考试平台已启动：端口 ${PORT}`);
      if (process.env.DATABASE_URL?.trim()) {
        console.log("数据存储：PostgreSQL（DATABASE_URL）");
      } else {
        console.log(`数据存储：SQLite 文件 ${SQLITE_PATH}`);
      }
    });
  })
  .catch((err) => {
    console.error("启动失败：", err);
    process.exit(1);
  });
