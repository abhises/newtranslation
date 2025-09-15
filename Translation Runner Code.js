/**
 * =================================================================================================
 * Monolithic i18n Bulk Translation Runner (Node.js) — with:
 *  - Full logging (before & after every step)
 *  - Directory scanner (i18n/*/en.json)
 *  - Bulk AWS Translate (batch; per-string fallback)
 *  - Uses your AwsS3 class (SDK v3 wrapper) from ./s3config.js for ALL S3 I/O
 *  - Auto-cleanup of S3 inputs/outputs after each locale is finished
 *  - Global progress % across ALL modules × locales
 *
 * ENV (examples)
 *  - NODE_ENV=local
 *  - LOGGING_ENABLED=1
 *  - LOGGING_CONSOLE_ENABLED=1
 *  - REGION=ap-southeast-2
 *  - S3_BUCKET=your-bucket
 *  - S3_INPUT_PREFIX=translations/input
 *  - S3_OUTPUT_PREFIX=translations/output
 *  - TRANSLATION_OUTPUT_ROOT=translations/jobs
 *  - I18N_BASE_DIR=i18n
 *  - TRANSLATE_ROLE_ARN=arn:aws:iam::<acct>:role/TranslateDataAccessRole
 *  - TRANSLATE_SYNC_FALLBACK=0            // 1 = force per-string fallback
 *  - SIMULATE_I18N=0                      // 1 = seed demo en.json into i18n/dashboard & i18n/profile
 *
 * Notes
 *  - This file REQUIRES your "./s3config.js" (AwsS3) and your logger (utils/UtilityLogger or Logger Final.js).
 *  - We validate presence of objects/functions before using them to prevent runtime errors.
 *  - We log *everything*: starts/ends, payload building, S3 upload, batch start/wait/fetch, write, validate, cleanup.
 *  - We compute a global % = (completed module-locale pairs) / (total pairs).
 * =================================================================================================
 */

const fs = require("fs");
const fsp = fs.promises;
const path = require("path");

// ----------------------------------------
// Logger (robust shim)
// ----------------------------------------
function getLogger() {
  try {
    const p1 = path.resolve(process.cwd(), "utils/UtilityLogger.js");
    if (fs.existsSync(p1)) {
      const m = require(p1);
      return typeof m?.writeLog === "function" ? m : { writeLog: (x) => console.log("(LoggerUtil)", x) };
    }
  } catch (_) {}
  try {
    const p2 = path.resolve(process.cwd(), "Logger Final.js");
    if (fs.existsSync(p2)) {
      const m = require(p2);
      return typeof m?.writeLog === "function" ? m : { writeLog: (x) => console.log("(LoggerFile)", x) };
    }
  } catch (_) {}
  return {
    writeLog: (payload) => {
      const safe = payload || {};
      const flag = safe.flag ? `[${safe.flag}]` : "[log]";
      console.log(`(LoggerShim) ${flag}`, JSON.stringify(safe));
    },
    isConsoleEnabled: () => true
  };
}
const Logger = getLogger();

// ----------------------------------------
// AwsS3 (your class, SDK v3 wrapper)
// ----------------------------------------
let AwsS3 = null;
try {
  const s3mod = path.resolve(process.cwd(), "s3config.js");
  if (!fs.existsSync(s3mod)) throw new Error("s3config.js not found next to this file");
  AwsS3 = require(s3mod); // class AwsS3
} catch (err) {
  Logger.writeLog({
    flag: "tr_error",
    action: "require_s3",
    message: `Failed to load s3config.js: ${err.message}`,
    critical: true,
    data: { time: Date.now() }
  });
  throw err;
}

// ----------------------------------------
// AWS Translate (SDK v2) — used just for Translate
// ----------------------------------------
let AWS = null;
try {
  const modPath = require.resolve("aws-sdk");
  AWS = modPath ? require("aws-sdk") : null;
} catch (_) {
  AWS = null;
}
if (!AWS || typeof AWS.Translate !== "function") {
  Logger.writeLog({
    flag: "tr_error",
    action: "aws_sdk_translate",
    message: "aws-sdk v2 not installed or AWS.Translate missing",
    critical: true,
    data: { time: Date.now() }
  });
  throw new Error("aws-sdk v2 with Translate is required");
}

// ----------------------------------------
// Config: Supported locales
// ----------------------------------------
const LOCALES = {
  source: { folderCode: "en", awsCode: "en", name: "English" },
  targets: [
    { folderCode: "ph", awsCode: "tl", name: "Filipino (Tagalog, Philippines)" },
    { folderCode: "vi", awsCode: "vi", name: "Vietnamese" }
    // add more if needed
  ]
};

// ----------------------------------------
// JSON helpers
// ----------------------------------------
const PLACEHOLDER_REGEX = /\{([^{}]+)\}/g;
function flattenJson(obj, prefix = "", out = {}) {
  if (obj == null || typeof obj !== "object") return out;
  for (const [k, v] of Object.entries(obj)) {
    const key = prefix ? `${prefix}.${k}` : k;
    if (v != null && typeof v === "object" && !Array.isArray(v)) {
      flattenJson(v, key, out);
    } else {
      out[key] = v;
    }
  }
  return out;
}
function unflattenJson(flat) {
  const root = {};
  for (const [k, v] of Object.entries(flat)) {
    const parts = k.split(".");
    let cur = root;
    for (let i = 0; i < parts.length; i++) {
      const part = parts[i];
      const isLeaf = i === parts.length - 1;
      if (!Object.prototype.hasOwnProperty.call(cur, part)) {
        cur[part] = isLeaf ? v : {};
      } else if (isLeaf) {
        cur[part] = v;
      }
      cur = cur[part];
    }
  }
  return root;
}
function extractPlaceholders(str) {
  if (typeof str !== "string") return new Set();
  const set = new Set(); let m;
  while ((m = PLACEHOLDER_REGEX.exec(str))) set.add(m[1]);
  return set;
}
function validateKeyAndPlaceholderParity({ sourceFlat, targetFlat }) {
  const errors = []; const missing = []; const extra = [];
  for (const key of Object.keys(sourceFlat)) (Object.prototype.hasOwnProperty.call(targetFlat, key) ? null : missing.push(key));
  for (const key of Object.keys(targetFlat)) (Object.prototype.hasOwnProperty.call(sourceFlat, key) ? null : extra.push(key));
  const placeholderDiffs = [];
  for (const key of Object.keys(sourceFlat)) {
    const srcPH = extractPlaceholders(sourceFlat[key]);
    const tgtPH = extractPlaceholders(targetFlat[key]);
    const missingPH = [...srcPH].filter((p) => !tgtPH.has(p));
    const extraPH = [...tgtPH].filter((p) => !srcPH.has(p));
    (missingPH.length || extraPH.length) ? placeholderDiffs.push({ key, missingPH, extraPH }) : null;
  }
  missing.length ? errors.push({ type: "missing_keys", keys: missing }) : null;
  extra.length   ? errors.push({ type: "extra_keys",   keys: extra }) : null;
  placeholderDiffs.length ? errors.push({ type: "placeholder_mismatch", items: placeholderDiffs }) : null;
  return { ok: errors.length === 0, errors };
}

// ----------------------------------------
// FS helpers + scanner
// ----------------------------------------
async function pathExists(p) { try { await fsp.access(p); return true; } catch { return false; } }
async function ensureDir(p) { (await pathExists(p)) ? null : await fsp.mkdir(p, { recursive: true }); }
async function readJsonSafe(file) { const ok = await pathExists(file); if (!ok) return null; const raw = await fsp.readFile(file, "utf8"); try { return JSON.parse(raw); } catch { return null; } }
async function writeJsonPretty(file, data) { await ensureDir(path.dirname(file)); const content = JSON.stringify(data, null, 2); await fsp.writeFile(file, content, "utf8"); }
async function scanI18nBaseDirs(explicitBaseDir) {
  const candidates = []; explicitBaseDir ? candidates.push(explicitBaseDir) : null;
  candidates.push(path.join(process.cwd(), "i18n")); candidates.push(path.join(process.cwd(), "18n"));
  const existing = []; for (const d of candidates) (await pathExists(d)) ? existing.push(d) : null;
  return [...new Set(existing)];
}
async function scanModulesWithEnglish(baseDir) {
  const results = []; const entries = await fsp.readdir(baseDir, { withFileTypes: true });
  for (const ent of entries) {
    if (!ent.isDirectory()) continue;
    const moduleDir = path.join(baseDir, ent.name);
    const enFile = path.join(moduleDir, "en.json");
    (await pathExists(enFile)) ? results.push({ baseDir, moduleName: ent.name, moduleDir, sourceFile: enFile }) : null;
  }
  return results;
}

// ----------------------------------------
// Helper: stream -> string (for AwsS3.getFile Body)
// ----------------------------------------
async function streamToString(body) {
  if (!body) return "";
  if (typeof body.transformToString === "function") {
    // For some SDK v3 fetch-like body streams
    return body.transformToString();
  }
  return new Promise((resolve, reject) => {
    let data = "";
    body.setEncoding && body.setEncoding("utf8");
    body.on("data", (chunk) => (data += chunk));
    body.on("end", () => resolve(data));
    body.on("error", (e) => reject(e));
  });
}

// ----------------------------------------
// Aws-backed helper (S3 + Translate)
// ----------------------------------------
class AwsTranslateAndS3 {
  constructor({ region, bucket, s3InputPrefix, s3OutputPrefix, roleArn, sourceLang }) {
    this.region = region || process.env.REGION || "us-east-1";
    this.bucket = bucket;
    this.s3InputPrefix = s3InputPrefix;
    this.s3OutputPrefix = s3OutputPrefix;
    this.roleArn = roleArn;
    this.sourceLang = sourceLang;
    this.translate = new AWS.Translate({ region: this.region });

    // init your AwsS3
    if (AwsS3 && typeof AwsS3.init === "function") AwsS3.init(this.region);
  }

  // Upload JSONL payload via your AwsS3
  async putJsonl(bucket, key, lines) {
    const body = lines.map((o) => JSON.stringify(o)).join("\n");
    if (AwsS3 && typeof AwsS3.uploadFile === "function") {
      await AwsS3.uploadFile(bucket, key, body, "text/plain");
      return `s3://${bucket}/${key}`;
    }
    throw new Error("AwsS3.uploadFile is not available");
  }

  async startBatch({ inputS3Uri, outputS3Uri, targetLang }) {
    const params = {
      JobName: `i18n-${targetLang}-${Date.now()}`,
      DataAccessRoleArn: this.roleArn,
      InputDataConfig: { S3Uri: inputS3Uri, ContentType: "text/plain" },
      OutputDataConfig: { S3Uri: outputS3Uri },
      SourceLanguageCode: this.sourceLang,
      TargetLanguageCodes: [targetLang]
    };
    const resp = await this.translate.startTextTranslationJob(params).promise();
    return { jobId: resp.JobId, jobName: params.JobName };
  }

  async waitForBatchCompletion({ jobId, logger, pollMs = 4000 }) {
    const log = (payload) => (logger && typeof logger.writeLog === "function" ? logger.writeLog(payload) : null);
    while (true) {
      const details = await this.translate.describeTextTranslationJob({ JobId: jobId }).promise();
      const status = details.TextTranslationJobProperties.JobStatus;
      log({ flag: "tr_batch_wait", action: "poll_status", message: `Batch job status: ${status}`, data: { jobId, time: Date.now() } });
      if (status === "COMPLETED") return details;
      if (status === "FAILED" || status === "STOPPED") {
        const reason = details.TextTranslationJobProperties.Message || "unknown";
        const err = new Error(`Batch translate job ${jobId} ${status}: ${reason}`);
        err.name = "BatchTranslateFailed";
        throw err;
      }
      await new Promise((r) => setTimeout(r, pollMs));
    }
  }

  // Fetch batch results via your AwsS3
  async fetchBatchResults({ outputS3Uri }) {
    const m = /^s3:\/\/([^/]+)\/(.+)$/.exec(outputS3Uri);
    if (!m) throw new Error(`Invalid S3 URI: ${outputS3Uri}`);
    const bucket = m[1]; const prefix = m[2].replace(/\/+$/g, "") + "/";

    const files = await AwsS3.listFiles(bucket, prefix);
    const cand = (files || []).find((o) => /\.out$/.test(o.Key)) || (files || [])[0];
    if (!cand) throw new Error(`No batch outputs under ${outputS3Uri}`);

    const body = await AwsS3.getFile(bucket, cand.Key);
    const text = await streamToString(body);
    const lines = text.trim().split(/\r?\n/);
    // Support either JSON-per-line or plaintext-per-line
    return lines.map((line) => {
      try { return JSON.parse(line); } catch { return { text: line }; }
    });
  }

  // Per-string fallback
  async translateText(text, targetLang) {
    const resp = await this.translate.translateText({
      Text: String(text ?? ""),
      SourceLanguageCode: this.sourceLang,
      TargetLanguageCode: targetLang
    }).promise();
    return resp.TranslatedText;
  }

  // Cleanup helpers
  async deleteInputObject(bucket, key) {
    return AwsS3 && typeof AwsS3.deleteFile === "function" ? AwsS3.deleteFile(bucket, key) : null;
  }
  async deleteOutputPrefix(bucket, prefix) {
    // list & bulk delete
    const files = await AwsS3.listFiles(bucket, prefix);
    if (files && files.length) {
      const keys = files.map((f) => f.Key);
      await AwsS3.deleteFiles(bucket, keys);
    }
  }
}

// ----------------------------------------
// TranslationRunner
// ----------------------------------------
class TranslationRunner {
  constructor(opts = {}) {
    this.now = new Date();
    this.tsFolder = this._ts(this.now);

    this.region = opts.region || process.env.REGION || "us-east-1";
    this.i18nBasePref = process.env.I18N_BASE_DIR || opts.i18nBaseDir || null;
    this.outputRoot = process.env.TRANSLATION_OUTPUT_ROOT || opts.outputRoot || path.join("translations", "jobs");
    this.bucket = process.env.S3_BUCKET || opts.s3Bucket || "";
    this.inPrefix = process.env.S3_INPUT_PREFIX || opts.s3InputPrefix || "translations/input";
    this.outPrefix = process.env.S3_OUTPUT_PREFIX || opts.s3OutputPrefix || "translations/output";
    this.roleArn = process.env.TRANSLATE_ROLE_ARN || opts.roleArn || null;
    this.forceSync = String(process.env.TRANSLATE_SYNC_FALLBACK || "").trim() === "1" ? true : !!opts.forceSync;

    this.source = LOCALES.source;
    this.targets = Array.isArray(LOCALES.targets) ? LOCALES.targets : [];

    this.aws = new AwsTranslateAndS3({
      region: this.region,
      bucket: this.bucket,
      s3InputPrefix: this.inPrefix,
      s3OutputPrefix: this.outPrefix,
      roleArn: this.roleArn,
      sourceLang: this.source.awsCode
    });

    this._log("tr_runner_start", "init", "Runner init", {
      region: this.region, outputRoot: this.outputRoot, bucket: this.bucket,
      inPrefix: this.inPrefix, outPrefix: this.outPrefix, roleArn: this.roleArn, forceSync: this.forceSync
    });

    // progress tracking
    this.totalPairs = 0;
    this.completedPairs = 0;
  }

  _ts(d) { const pad = (n) => String(n).padStart(2, "0"); return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}_${pad(d.getHours())}-${pad(d.getMinutes())}-${pad(d.getSeconds())}`; }

  _log(flag, action, message, data = {}, critical = false) {
    const payload = { flag, action, message, critical, data: { ...data, time: Date.now() } };
    typeof Logger?.writeLog === "function" ? Logger.writeLog(payload) : console.log(payload);
  }

  async _step({ startFlag, endFlag, action, messageStart, messageEnd, data = {}, fn }) {
    this._log(startFlag, action, messageStart, data);
    const result = await fn();
    this._log(endFlag, action, messageEnd, data);
    return result;
  }

  _progressTick(extra = {}) {
    const percent = this.totalPairs > 0 ? Math.round((this.completedPairs / this.totalPairs) * 100) : 0;
    this._log("tr_progress", "global", `Global progress: ${this.completedPairs}/${this.totalPairs} (${percent}%)`, { percent, ...extra });
  }

  async ensureJobDir() {
    const job = path.join(this.outputRoot, this.tsFolder);
    await this._step({
      startFlag: "tr_job_dir_start",
      endFlag: "tr_job_dir_end",
      action: "mkdir",
      messageStart: `Ensuring job dir: ${job}`,
      messageEnd: `Job dir ready`,
      data: { job },
      fn: async () => ensureDir(job)
    });
    return job;
  }

  async simulateI18nIfRequested(baseDir) {
    if (String(process.env.SIMULATE_I18N || "").trim() !== "1") return;
    const demo = {
      auth: { login: { title: "Login to Your Account", button: "Login" } },
      dashboard: { page: { title: "Dashboard" }, messages: { welcome: "Welcome, {0}!" } },
      profile: { header: { title: "Profile" }, action: { save: "Save changes" } }
    };
    const dash = path.join(baseDir, "dashboard");
    const profile = path.join(baseDir, "profile");
    await ensureDir(dash); await ensureDir(profile);
    await writeJsonPretty(path.join(dash, "en.json"), demo);
    await writeJsonPretty(path.join(profile, "en.json"), demo);
  }

  async scanAllModules() {
    this._log("tr_scan_start", "scan", "Scanning base dirs", { hint: this.i18nBasePref || "auto" });
    const baseDirs = await scanI18nBaseDirs(this.i18nBasePref);
    if (!baseDirs.length) throw new Error(`No i18n/18n base directory found`);
    const found = [];
    for (const base of baseDirs) {
      await this.simulateI18nIfRequested(base);
      const mods = await scanModulesWithEnglish(base);
      for (const m of mods) found.push({ ...m, baseDir: base });
    }
    this._log("tr_scan_found", "scan", `Found ${found.length} module(s) with en.json`, {
      files: found.map((m) => `/${path.basename(m.baseDir)}/${m.moduleName}/en.json`)
    });
    this._log("tr_scan_end", "scan", "Scanning completed", { baseDirs });
    return found;
  }

  _s3InputKey(moduleName, localeCode) {
    const uid = Math.random().toString(36).slice(2);
    return [this.inPrefix, this.tsFolder, moduleName, localeCode, `${uid}.jsonl`].filter(Boolean).join("/");
  }
  _s3OutputPrefix(moduleName, localeCode) {
    return [this.outPrefix, this.tsFolder, moduleName, localeCode].filter(Boolean).join("/") + "/";
  }
  _s3OutputUri(moduleName, localeCode) {
    return `s3://${this.bucket}/${this._s3OutputPrefix(moduleName, localeCode)}`;
  }
  _localOut(jobDir, moduleName, localeCode) { return path.join(jobDir, moduleName, `${localeCode}.json`); }

  async buildPayload(enFile) {
    return this._step({
      startFlag: "tr_payload_build_start",
      endFlag: "tr_payload_build_end",
      action: "flatten",
      messageStart: `Flattening ${enFile} -> JSONL`,
      messageEnd: `Payload built`,
      data: { enFile },
      fn: async () => {
        const json = await readJsonSafe(enFile);
        if (!json) throw new Error(`Cannot read or parse ${enFile}`);
        const flat = flattenJson(json);
        const lines = Object.entries(flat).map(([key, text]) => ({ key, text }));
        return { flatSource: flat, lines };
      }
    });
  }

  async translateModuleLocale({ moduleName, enFile, jobDir, target }) {
    const targetFolder = target.folderCode;
    const targetAws = target.awsCode;

    const { flatSource, lines } = await this.buildPayload(enFile);

    // Upload JSONL input to S3 (via your AwsS3)
    const inputKey = this._s3InputKey(moduleName, targetFolder);
    const inputUri = await this._step({
      startFlag: "tr_s3_upload_start",
      endFlag: "tr_s3_upload_end",
      action: "s3_put",
      messageStart: `Uploading input JSONL to s3://${this.bucket}/${inputKey}`,
      messageEnd: `Input uploaded`,
      data: { bucket: this.bucket, key: inputKey },
      fn: async () => this.aws.putJsonl(this.bucket, inputKey, lines)
    });

    const outputUri = this._s3OutputUri(moduleName, targetFolder);
    const outputPrefix = this._s3OutputPrefix(moduleName, targetFolder);

    // Try batch
    let translatedFlat = null;
    if (!this.forceSync && this.roleArn) {
      try {
        const { jobId, jobName } = await this._step({
          startFlag: "tr_batch_start",
          endFlag: "tr_batch_start",
          action: "batch_start",
          messageStart: `Starting batch translation`,
          messageEnd: `Batch started`,
          data: { moduleName, targetFolder, jobName, inputUri, outputUri, source: this.source.awsCode, target: targetAws },
          fn: async () => this.aws.startBatch({ inputS3Uri: inputUri, outputS3Uri: outputUri, targetLang: targetAws })
        });

        await this._step({
          startFlag: "tr_batch_wait",
          endFlag: "tr_batch_wait",
          action: "batch_wait",
          messageStart: `Waiting for job ${jobId}`,
          messageEnd: `Polling...`,
          data: { jobId },
          fn: async () => this.aws.waitForBatchCompletion({ jobId, logger: Logger })
        });

        const fetched = await this._step({
          startFlag: "tr_batch_fetch",
          endFlag: "tr_batch_end",
          action: "batch_fetch",
          messageStart: `Fetching batch results`,
          messageEnd: `Batch results fetched`,
          data: { outputUri },
          fn: async () => this.aws.fetchBatchResults({ outputS3Uri: outputUri })
        });

        translatedFlat = {};
        for (let i = 0; i < lines.length; i++) {
          const key = lines[i].key;
          const val = fetched[i] && typeof fetched[i].text === "string" ? fetched[i].text : fetched[i];
          translatedFlat[key] = typeof val === "string" ? val : String(val ?? "");
        }
      } catch (e) {
        this._log("tr_error", "batch_fail", `Batch failed; falling back to per-string. ${e.message}`, { moduleName, targetFolder }, true);
      }
    }

    // Fallback per-string
    if (!translatedFlat) {
      const out = await this._step({
        startFlag: "tr_sync_start",
        endFlag: "tr_sync_end",
        action: "sync_translate",
        messageStart: `Translating per-string`,
        messageEnd: `Per-string complete`,
        data: { moduleName, targetFolder },
        fn: async () => {
          const ret = [];
          for (const { key, text } of lines) {
            const t = await this.aws.translateText(text, targetAws);
            ret.push({ key, text: t });
          }
          return ret;
        }
      });
      translatedFlat = {}; for (const r of out) translatedFlat[r.key] = r.text;
    }

    // Convert to nested JSON
    const nested = await this._step({
      startFlag: "tr_convert_start",
      endFlag: "tr_convert_end",
      action: "unflatten",
      messageStart: `Converting flat -> nested`,
      messageEnd: `Converted nested`,
      data: { moduleName, targetFolder },
      fn: async () => unflattenJson(translatedFlat)
    });

    // Validate
    const { ok, errors } = validateKeyAndPlaceholderParity({ sourceFlat: flatSource, targetFlat: translatedFlat });
    await this._step({
      startFlag: "tr_validate_start",
      endFlag: ok ? "tr_validate_pass" : "tr_validate_fail",
      action: "validate",
      messageStart: `Validating`,
      messageEnd: ok ? `Validation passed` : `Validation failed`,
      data: { moduleName, targetFolder, errors },
      fn: async () => {}
    });
    if (!ok) {
      const err = new Error(`Validation failed for ${moduleName}/${targetFolder}: ${JSON.stringify(errors)}`);
      this._log("tr_error", "validate_fail", err.message, { moduleName, targetFolder }, true);
      throw err;
    }

    // Write local file
    const outPath = this._localOut(jobDir, moduleName, targetFolder);
    await this._step({
      startFlag: "tr_write_start",
      endFlag: "tr_write_end",
      action: "write_file",
      messageStart: `Writing ${outPath}`,
      messageEnd: `File written`,
      data: { outPath },
      fn: async () => writeJsonPretty(outPath, nested)
    });

    // --- CLEANUP S3 (delete input object + all output objects) ---
    await this._step({
      startFlag: "tr_cleanup_start",
      endFlag: "tr_cleanup_end",
      action: "s3_cleanup",
      messageStart: `Cleaning up S3 input & output`,
      messageEnd: `S3 cleanup complete`,
      data: { inputKey, outputPrefix },
      fn: async () => {
        // Delete input jsonl
        if (AwsS3 && typeof AwsS3.deleteFile === "function") {
          await AwsS3.deleteFile(this.bucket, inputKey);
        }
        // Delete all files under output prefix
        if (AwsS3 && typeof AwsS3.listFiles === "function" && typeof AwsS3.deleteFiles === "function") {
          const files = await AwsS3.listFiles(this.bucket, outputPrefix);
          if (files && files.length) {
            const keys = files.map((f) => f.Key);
            await AwsS3.deleteFiles(this.bucket, keys);
          }
        }
      }
    });

    return outPath;
  }

  async generateBulkTranslations() {
    const jobDir = await this.ensureJobDir();
    const modules = await this.scanAllModules();
    this.totalPairs = modules.length * this.targets.length;
    this.completedPairs = 0;
    this._progressTick({ stage: "init" });

    // Informative log per the requested runner narration
    this._log("tr_runner_start", "announce", "Runner will process (module/en) -> all target locales", {
      totalModules: modules.length,
      targets: this.targets.map((t) => t.folderCode)
    });

    for (const mod of modules) {
      const { moduleName, sourceFile } = mod;

      for (const target of this.targets) {
        const info = { moduleName, sourceFile, targetFolder: target.folderCode, targetAws: target.awsCode };
        this._log("tr_runner_start", "module_locale_begin", `Begin ${moduleName}/${target.folderCode}`, info);

        try {
          const saved = await this.translateModuleLocale({ moduleName, enFile: sourceFile, jobDir, target });
          this._log("tr_runner_end", "module_locale_end", `Completed ${moduleName}/${target.folderCode}`, { ...info, saved });
        } catch (e) {
          this._log("tr_error", "module_locale_error", e.message, { ...info }, true);
          // keep going
        } finally {
          this.completedPairs += 1;
          this._progressTick({ moduleName, targetFolder: target.folderCode });
        }
      }
    }

    this._log("tr_runner_end", "done", `All modules & locales processed`, { jobDir, total: this.totalPairs, completed: this.completedPairs });
    return jobDir;
  }
}

// ----------------------------------------
// CLI entry
// ----------------------------------------
if (require.main === module) {
  (async () => {
    try {
      const runner = new TranslationRunner({});
      const outDir = await runner.generateBulkTranslations();
      console.log(`✅ Translation job completed. Output at: ${outDir}`);
    } catch (err) {
      console.error(`❌ Translation job failed:`, err?.message || err);
      typeof Logger?.writeLog === "function"
        ? Logger.writeLog({ flag: "tr_error", action: "fatal", message: err?.message || String(err), critical: true, data: { time: Date.now() } })
        : null;
      process.exitCode = 1;
    }
  })();
}

// ----------------------------------------
// Exports (optional if you import this file elsewhere)
// ----------------------------------------
module.exports = {
  TranslationRunner,
  LOCALES,
  utils: {
    flattenJson,
    unflattenJson,
    validateKeyAndPlaceholderParity,
    extractPlaceholders,
    scanI18nBaseDirs,
    scanModulesWithEnglish,
    readJsonSafe,
    writeJsonPretty
  }
};
