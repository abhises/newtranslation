const LOCALES = {
  source: { folderCode: "en", awsCode: "en", name: "English" },
  targets: [
    {
      folderCode: "ph",
      awsCode: "tl",
      name: "Filipino (Tagalog, Philippines)",
    },
    { folderCode: "vi", awsCode: "vi", name: "Vietnamese" },
    // add more if needed
  ],
};

export default class TranslationRunner {
  constructor(opts = {}) {
    this.now = new Date();
    this.tsFolder = this._ts(this.now);

    this.region = opts.region || process.env.REGION || "us-east-1";
    this.i18nBasePref = process.env.I18N_BASE_DIR || opts.i18nBaseDir || null;
    this.outputRoot =
      process.env.TRANSLATION_OUTPUT_ROOT ||
      opts.outputRoot ||
      path.join("translations", "jobs");
    this.bucket = process.env.S3_BUCKET || opts.s3Bucket || "";
    this.inPrefix =
      process.env.S3_INPUT_PREFIX || opts.s3InputPrefix || "translations/input";
    this.outPrefix =
      process.env.S3_OUTPUT_PREFIX ||
      opts.s3OutputPrefix ||
      "translations/output";
    this.roleArn = process.env.TRANSLATE_ROLE_ARN || opts.roleArn || null;
    this.forceSync =
      String(process.env.TRANSLATE_SYNC_FALLBACK || "").trim() === "1"
        ? true
        : !!opts.forceSync;

    this.source = LOCALES.source;
    this.targets = Array.isArray(LOCALES.targets) ? LOCALES.targets : [];

    this.aws = new AwsTranslateAndS3({
      region: this.region,
      bucket: this.bucket,
      s3InputPrefix: this.inPrefix,
      s3OutputPrefix: this.outPrefix,
      roleArn: this.roleArn,
      sourceLang: this.source.awsCode,
    });

    this._log("tr_runner_start", "init", "Runner init", {
      region: this.region,
      outputRoot: this.outputRoot,
      bucket: this.bucket,
      inPrefix: this.inPrefix,
      outPrefix: this.outPrefix,
      roleArn: this.roleArn,
      forceSync: this.forceSync,
    });

    // progress tracking
    this.totalPairs = 0;
    this.completedPairs = 0;
  }

  _ts(d) {
    const pad = (n) => String(n).padStart(2, "0");
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(
      d.getDate()
    )}_${pad(d.getHours())}-${pad(d.getMinutes())}-${pad(d.getSeconds())}`;
  }

  _log(flag, action, message, data = {}, critical = false) {
    const payload = {
      flag,
      action,
      message,
      critical,
      data: { ...data, time: Date.now() },
    };
    typeof Logger?.writeLog === "function"
      ? Logger.writeLog(payload)
      : console.log(payload);
  }

  async _step({
    startFlag,
    endFlag,
    action,
    messageStart,
    messageEnd,
    data = {},
    fn,
  }) {
    this._log(startFlag, action, messageStart, data);
    const result = await fn();
    this._log(endFlag, action, messageEnd, data);
    return result;
  }

  _progressTick(extra = {}) {
    const percent =
      this.totalPairs > 0
        ? Math.round((this.completedPairs / this.totalPairs) * 100)
        : 0;
    this._log(
      "tr_progress",
      "global",
      `Global progress: ${this.completedPairs}/${this.totalPairs} (${percent}%)`,
      { percent, ...extra }
    );
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
      fn: async () => ensureDir(job),
    });
    return job;
  }

  async simulateI18nIfRequested(baseDir) {
    if (String(process.env.SIMULATE_I18N || "").trim() !== "1") return;
    const demo = {
      auth: { login: { title: "Login to Your Account", button: "Login" } },
      dashboard: {
        page: { title: "Dashboard" },
        messages: { welcome: "Welcome, {0}!" },
      },
      profile: {
        header: { title: "Profile" },
        action: { save: "Save changes" },
      },
    };
    const dash = path.join(baseDir, "dashboard");
    const profile = path.join(baseDir, "profile");
    await ensureDir(dash);
    await ensureDir(profile);
    await writeJsonPretty(path.join(dash, "en.json"), demo);
    await writeJsonPretty(path.join(profile, "en.json"), demo);
  }

  async scanAllModules() {
    this._log("tr_scan_start", "scan", "Scanning base dirs", {
      hint: this.i18nBasePref || "auto",
    });
    const baseDirs = await scanI18nBaseDirs(this.i18nBasePref);
    if (!baseDirs.length) throw new Error(`No i18n/18n base directory found`);
    const found = [];
    for (const base of baseDirs) {
      await this.simulateI18nIfRequested(base);
      const mods = await scanModulesWithEnglish(base);
      for (const m of mods) found.push({ ...m, baseDir: base });
    }
    this._log(
      "tr_scan_found",
      "scan",
      `Found ${found.length} module(s) with en.json`,
      {
        files: found.map(
          (m) => `/${path.basename(m.baseDir)}/${m.moduleName}/en.json`
        ),
      }
    );
    this._log("tr_scan_end", "scan", "Scanning completed", { baseDirs });
    return found;
  }

  _s3InputKey(moduleName, localeCode) {
    const uid = Math.random().toString(36).slice(2);
    return [
      this.inPrefix,
      this.tsFolder,
      moduleName,
      localeCode,
      `${uid}.jsonl`,
    ]
      .filter(Boolean)
      .join("/");
  }
  _s3OutputPrefix(moduleName, localeCode) {
    return (
      [this.outPrefix, this.tsFolder, moduleName, localeCode]
        .filter(Boolean)
        .join("/") + "/"
    );
  }
  _s3OutputUri(moduleName, localeCode) {
    return `s3://${this.bucket}/${this._s3OutputPrefix(
      moduleName,
      localeCode
    )}`;
  }
  _localOut(jobDir, moduleName, localeCode) {
    return path.join(jobDir, moduleName, `${localeCode}.json`);
  }

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
        const lines = Object.entries(flat).map(([key, text]) => ({
          key,
          text,
        }));
        return { flatSource: flat, lines };
      },
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
      fn: async () => this.aws.putJsonl(this.bucket, inputKey, lines),
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
          data: {
            moduleName,
            targetFolder,
            jobName,
            inputUri,
            outputUri,
            source: this.source.awsCode,
            target: targetAws,
          },
          fn: async () =>
            this.aws.startBatch({
              inputS3Uri: inputUri,
              outputS3Uri: outputUri,
              targetLang: targetAws,
            }),
        });

        await this._step({
          startFlag: "tr_batch_wait",
          endFlag: "tr_batch_wait",
          action: "batch_wait",
          messageStart: `Waiting for job ${jobId}`,
          messageEnd: `Polling...`,
          data: { jobId },
          fn: async () =>
            this.aws.waitForBatchCompletion({ jobId, logger: Logger }),
        });

        const fetched = await this._step({
          startFlag: "tr_batch_fetch",
          endFlag: "tr_batch_end",
          action: "batch_fetch",
          messageStart: `Fetching batch results`,
          messageEnd: `Batch results fetched`,
          data: { outputUri },
          fn: async () =>
            this.aws.fetchBatchResults({ outputS3Uri: outputUri }),
        });

        translatedFlat = {};
        for (let i = 0; i < lines.length; i++) {
          const key = lines[i].key;
          const val =
            fetched[i] && typeof fetched[i].text === "string"
              ? fetched[i].text
              : fetched[i];
          translatedFlat[key] =
            typeof val === "string" ? val : String(val ?? "");
        }
      } catch (e) {
        this._log(
          "tr_error",
          "batch_fail",
          `Batch failed; falling back to per-string. ${e.message}`,
          { moduleName, targetFolder },
          true
        );
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
        },
      });
      translatedFlat = {};
      for (const r of out) translatedFlat[r.key] = r.text;
    }

    // Convert to nested JSON
    const nested = await this._step({
      startFlag: "tr_convert_start",
      endFlag: "tr_convert_end",
      action: "unflatten",
      messageStart: `Converting flat -> nested`,
      messageEnd: `Converted nested`,
      data: { moduleName, targetFolder },
      fn: async () => unflattenJson(translatedFlat),
    });

    // Validate
    const { ok, errors } = validateKeyAndPlaceholderParity({
      sourceFlat: flatSource,
      targetFlat: translatedFlat,
    });
    await this._step({
      startFlag: "tr_validate_start",
      endFlag: ok ? "tr_validate_pass" : "tr_validate_fail",
      action: "validate",
      messageStart: `Validating`,
      messageEnd: ok ? `Validation passed` : `Validation failed`,
      data: { moduleName, targetFolder, errors },
      fn: async () => {},
    });
    if (!ok) {
      const err = new Error(
        `Validation failed for ${moduleName}/${targetFolder}: ${JSON.stringify(
          errors
        )}`
      );
      this._log(
        "tr_error",
        "validate_fail",
        err.message,
        { moduleName, targetFolder },
        true
      );
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
      fn: async () => writeJsonPretty(outPath, nested),
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
        if (
          AwsS3 &&
          typeof AwsS3.listFiles === "function" &&
          typeof AwsS3.deleteFiles === "function"
        ) {
          const files = await AwsS3.listFiles(this.bucket, outputPrefix);
          if (files && files.length) {
            const keys = files.map((f) => f.Key);
            await AwsS3.deleteFiles(this.bucket, keys);
          }
        }
      },
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
    this._log(
      "tr_runner_start",
      "announce",
      "Runner will process (module/en) -> all target locales",
      {
        totalModules: modules.length,
        targets: this.targets.map((t) => t.folderCode),
      }
    );

    for (const mod of modules) {
      const { moduleName, sourceFile } = mod;

      for (const target of this.targets) {
        const info = {
          moduleName,
          sourceFile,
          targetFolder: target.folderCode,
          targetAws: target.awsCode,
        };
        this._log(
          "tr_runner_start",
          "module_locale_begin",
          `Begin ${moduleName}/${target.folderCode}`,
          info
        );

        try {
          const saved = await this.translateModuleLocale({
            moduleName,
            enFile: sourceFile,
            jobDir,
            target,
          });
          this._log(
            "tr_runner_end",
            "module_locale_end",
            `Completed ${moduleName}/${target.folderCode}`,
            { ...info, saved }
          );
        } catch (e) {
          this._log(
            "tr_error",
            "module_locale_error",
            e.message,
            { ...info },
            true
          );
          // keep going
        } finally {
          this.completedPairs += 1;
          this._progressTick({ moduleName, targetFolder: target.folderCode });
        }
      }
    }

    this._log("tr_runner_end", "done", `All modules & locales processed`, {
      jobDir,
      total: this.totalPairs,
      completed: this.completedPairs,
    });
    return jobDir;
  }
}
