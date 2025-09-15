import dotenv from "dotenv";
import AwsS3 from "./AwsS3.js";

dotenv.config();

export default class AwsTranslateAndS3 {
  constructor({
    region,
    bucket,
    s3InputPrefix,
    s3OutputPrefix,
    roleArn,
    sourceLang,
  }) {
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
      TargetLanguageCodes: [targetLang],
    };
    const resp = await this.translate.startTextTranslationJob(params).promise();
    return { jobId: resp.JobId, jobName: params.JobName };
  }

  async waitForBatchCompletion({ jobId, logger, pollMs = 4000 }) {
    const log = (payload) =>
      logger && typeof logger.writeLog === "function"
        ? logger.writeLog(payload)
        : null;
    while (true) {
      const details = await this.translate
        .describeTextTranslationJob({ JobId: jobId })
        .promise();
      const status = details.TextTranslationJobProperties.JobStatus;
      log({
        flag: "tr_batch_wait",
        action: "poll_status",
        message: `Batch job status: ${status}`,
        data: { jobId, time: Date.now() },
      });
      if (status === "COMPLETED") return details;
      if (status === "FAILED" || status === "STOPPED") {
        const reason =
          details.TextTranslationJobProperties.Message || "unknown";
        const err = new Error(
          `Batch translate job ${jobId} ${status}: ${reason}`
        );
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
    const bucket = m[1];
    const prefix = m[2].replace(/\/+$/g, "") + "/";

    const files = await AwsS3.listFiles(bucket, prefix);
    const cand =
      (files || []).find((o) => /\.out$/.test(o.Key)) || (files || [])[0];
    if (!cand) throw new Error(`No batch outputs under ${outputS3Uri}`);

    const body = await AwsS3.getFile(bucket, cand.Key);
    const text = await streamToString(body);
    const lines = text.trim().split(/\r?\n/);
    // Support either JSON-per-line or plaintext-per-line
    return lines.map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return { text: line };
      }
    });
  }

  // Per-string fallback
  async translateText(text, targetLang) {
    const resp = await this.translate
      .translateText({
        Text: String(text ?? ""),
        SourceLanguageCode: this.sourceLang,
        TargetLanguageCode: targetLang,
      })
      .promise();
    return resp.TranslatedText;
  }

  // Cleanup helpers
  async deleteInputObject(bucket, key) {
    return AwsS3 && typeof AwsS3.deleteFile === "function"
      ? AwsS3.deleteFile(bucket, key)
      : null;
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
