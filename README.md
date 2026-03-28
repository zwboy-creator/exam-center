# 员工手册学习考试平台（MVP）

## 1. 功能概览
- 手机号注册/登录：员工自行注册，手机号作为唯一登录ID
- 注册强约束：门店、部门、岗位必须从系统已配置项中选择
- 在线考试：自动组卷（通用60分 + 岗位40分）、60分钟倒计时、自动评分
- 在线进度保存：答题与题目标记自动保存到服务端，支持跨设备继续作答
- 补考规则：不合格可补考1次（共2次机会）
- 成绩明细：提交后立即显示分数、合格状态、每题对错
- 管理后台：用户管理、用户批量导入、基础数据配置、题库导入
- 门店统计：注册人数、参与人数、通过人数、参与率、通过率、平均分

## 2. 数据来源
启动时会自动从 `seed/` 导入以下基础数据（仅当系统表为空时）：
- `seed/stores_rows.csv`
- `seed/departments_rows.csv`
- `seed/positions_rows.csv`
- `seed/questions_rows.csv`

注意：**系统不会初始化员工用户数据**。`src/data/app-data.json` 首次启动后 `users` 默认为空。

## 3. 启动方式
```bash
npm install
npm start
```
默认地址：`http://localhost:3000`

## 4. 首次使用
1. 打开 `http://localhost:3000/setup` 创建首个管理员
2. 管理员登录后可进入：
   - 用户管理：`/admin/users`
   - 批量导入：`/admin/users/import`
   - 基础配置：`/admin/config`
   - 题库管理：`/admin/questions`
3. 员工通过 `http://localhost:3000/register` 自行注册后参加考试

补充说明：
- `/setup` 仅在“系统没有启用中的管理员”时开放，用于首次初始化。
- 若系统已存在管理员，需登录后台到“用户管理”新增其他管理员（角色选择 `admin`）。

## 5. 批量导入用户模板
可参考：`seed/users_import_template.csv`

支持中英文列名（常用）：
- 姓名：`name` / `姓名`
- 手机号：`phone` / `手机号`
- 门店：`store_id` / `store_name` / `门店` / `所在门店`
- 部门：`department_id` / `department_name` / `部门` / `所在部门`
- 岗位：`position_id` / `position_name` / `岗位` / `所在岗位`
- 角色：`role`（`employee`/`manager`/`admin`）
- 状态：`status`（`active`/`disabled`）
- 密码：`password`（不填则默认手机号后6位）

## 6. 数据文件
- 主数据文件：`src/data/app-data.json`
- 该文件包含：用户、考试记录、成绩、日志、基础配置、题库

## 7. 说明
- 为保证中文CSV兼容，系统按 `GB18030` 解码种子CSV
- 当前版本为快速可用MVP，适合先上线内测与流程验证
